
import os
from datetime import datetime, timezone

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv


def main():
    load_dotenv()

    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    filesystem = os.environ["ADLS_FILESYSTEM"]

    credential = AzureCliCredential()

    try:
        service = DataLakeServiceClient(
            account_url=f"https://{account}.dfs.core.windows.net",
            credential=credential,
        )

        fs_client = service.get_file_system_client(filesystem)

        # Verify container access
        fs_client.get_file_system_properties()
        print("Connected to ADLS Gen2 successfully!")

        # Upload a small test file
        path = "raw/connection-test/hello.txt"
        file_client = fs_client.get_file_client(path)

        content = (
            "CarePulse ADLS connection test\n"
            f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n"
        )

        file_client.upload_data(
            content.encode("utf-8"),
            overwrite=True,
        )

        print(f"Uploaded: {path}")

        # Verify by downloading
        downloaded = (
            file_client.download_file()
            .readall()
            .decode("utf-8")
        )

        assert downloaded == content

        print("Download verification successful!")
        print("ADLS Gen2 connection test PASSED")

    finally:
        credential.close()


if __name__ == "__main__":
    main()
