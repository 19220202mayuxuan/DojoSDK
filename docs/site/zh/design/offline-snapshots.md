# 离线 Parquet 快照

`SnapshotResolver` 将 provider 下载与数据加载分离。它不读取 Parquet 行，
也不会暴露下载不完整的仓库。

## 存储与提交协议

已提交的文件存放在：

```text
<cache_dir>/dojosdk/snapshots/<backend>/<repo>/
  active.json
  revisions/<resolved-revision>-<token>-<file-manifest>/...
  .staging/<unique-id>/...
```

对每个仓库，所有请求的文件先下载到唯一的 staging 目录，检查文件大小为正
且 Parquet footer 有效，然后通过一次原子目录重命名移动到 `revisions/`。
只有整个 revision 校验通过后，`active.json` 才会被原子替换。失败的更新
不会触碰旧的指针和文件。

Provider 缓存文件通过链接或复制进入 DojoSDK 自有的快照。resolver 永远
不会删除 Hugging Face 或 ModelScope 公共缓存的 revision。

每个进行中的 staging 目录都持有进程租约（process lease）。在新同步之前，
resolver 只删除超过 `DOJO_SNAPSHOT_STAGING_TTL`（默认七天）且属主进程
已不存在的 staging 目录。没有有效租约的目录会被保留，因为其归属无法证明。
对未变化的 `active.json` 的重复读取使用有界的进程内缓存：每个活跃指针
代际只校验一次 Parquet footer，后续读取只做廉价的 existence/size 检查。
`active.json` 的原子替换会使该缓存失效。

已提交的 `revisions/` 不会被自动删除。清理是显式的管理操作，默认 dry-run：

```python
preview = client.gc_offline_snapshots(
    paths=["/api/qdata/v1/stock/kline"],
    min_age_seconds=7 * 24 * 60 * 60,
    keep_previous=1,
)

result = client.gc_offline_snapshots(
    paths=["/api/qdata/v1/stock/kline"],
    min_age_seconds=7 * 24 * 60 * 60,
    keep_previous=1,
    dry_run=False,
)
```

GC 使用每仓库跨进程锁，始终保留活跃 revision，保留指定数量的最近
previous revision，跳过年轻于 `min_age_seconds` 的 revision，并跳过
持有活跃查询租约的 revision。它只触碰 DojoSDK 自有的 `revisions/`
目录，绝不删除 Hugging Face 或 ModelScope 公共缓存。调用方必须像同步
一样显式选择 paths 或 repositories。

## Revision 身份

- Hugging Face 快照使用不可变的仓库 commit SHA。
- ModelScope 的文件树元数据暴露的是每文件 revision 而非单一仓库 head
  SHA。因此 resolver 将所需文件的 `Revision`、`Sha256`、`Size` 元数据
  哈希成稳定的 manifest revision，并在下载后再次校验 manifest。
- 无远程元数据的本地引导（local-only bootstrapping）使用内容 manifest。

公开的 `snapshot_token` 包含 backend、provider repository、resolved
revision 和快照布局版本。多仓库消费方应对查询开始时捕获的 `SnapshotRef`
对象使用 `combine_snapshot_tokens()`。

## 公开 API

`Dojo` 与 `AsyncDojo` 均暴露：

```python
ensure_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])

sync_offline_snapshots(
    repos=["AlphaDojo/dojo_stock_kline"],
    check_remote=True,
    force=False,
)

gc_offline_snapshots(
    repos=["AlphaDojo/dojo_stock_kline"],
    dry_run=True,
)
```

`paths` 或 `repos` 至少要有一个非空参数，这是强制的。两者都省略绝不意味着
"下载完整注册表"。`force=True` 保留给显式的修复或管理工作流。
