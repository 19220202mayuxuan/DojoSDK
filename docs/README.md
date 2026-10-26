# DojoSDK 文档站维护说明

本目录包含 DojoSDK 的 MkDocs 在线文档源码。除本维护说明外，所有可发布
文档和文档资源都必须放在 `docs/site/` 中，禁止再在 `docs/` 根目录新增
独立设计文档。

## 目录结构

```text
docs/
├── README.md                  # 仅供维护者阅读的文档治理说明
└── site/                      # MkDocs 的唯一 docs_dir
    ├── assets/                # 中英文共享的静态资源
    │   └── openapi/           # OpenAPI JSON（qdata_openapi.json）
    ├── zh/                    # 中文站点，默认发布到 /
    │   ├── design/            # 当前有效的子系统设计与契约
    │   ├── reference/         # 稳定接口、弃用清单与移除门槛
    │   ├── development/       # 开发、测试和验证操作
    │   └── plans/             # 实施计划、迁移步骤和历史决策
    └── en/                    # 英文站点；与 zh 保持相同相对路径
```

## 分类边界

- `design/`：记录一个具体子系统当前有效的设计、契约和行为边界。
- `reference/`：记录调用方可以依赖的稳定接口、弃用安排和移除门槛。
- `development/`：记录如何开发、测试、验证和发布。
- `plans/`：记录尚未完成的方案、迁移顺序、backlog 或历史决策；它不是
  当前 API 契约。
- `assets/`：只存放站点共享静态资源，不存放 Markdown 正文。

计划实施完成后，要把长期有效的结论迁移到 `design/`、`reference/` 或
`development/`，并删除或归档失效计划。不要让同一份权威说明同时存在于
多个分类。

中英文页面保持相同的相对路径；新增或移动已发布页面时同步更新根目录
`mkdocs.yml` 的导航。

金融数据离线通路的权威说明在正式站：

- `docs/site/*/design/offline-snapshots.md`（快照存储、提交协议与 GC）
- `docs/site/*/design/offline-kline-s5.md`（有界批量 K 线契约）
- `docs/site/*/reference/offline-deprecations.md`（弃用清单与移除门槛）
