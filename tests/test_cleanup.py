from dojo.datasource.huggingface import HuggingFaceDataSource
import os

def main() -> None:
    os.environ["DOJO_LOG_LEVEL"] = "INFO"
    ds = HuggingFaceDataSource()
    result = ds.cleanup_cache()
    print("Cleanup Result:", result)


if __name__ == "__main__":
    main()
