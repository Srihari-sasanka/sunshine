"""
DynamoDB helper for Sunshine EV Hub.
Encapsulates table creation, product seeding, and CRUD operations for:
- PrimaryUsers
- LeaseRequests
- Products
"""

import os
import uuid
from decimal import Decimal
from typing import Dict, List, Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError


def _to_decimal_safe(value):
    """Convert numeric values for DynamoDB compatibility."""
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    return value


class Database:
    def __init__(self, region_name=None):
        # try to load local aws_config.py if present
        try:
            import aws_config as _local_cfg
        except Exception:
            _local_cfg = None

        self.region = region_name or os.environ.get("AWS_REGION") or (
            getattr(_local_cfg, "AWS_REGION", "ap-southeast-2") if _local_cfg else "ap-southeast-2"
        )

        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID") or (
            getattr(_local_cfg, "AWS_ACCESS_KEY_ID", None) if _local_cfg else None
        )
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or (
            getattr(_local_cfg, "AWS_SECRET_ACCESS_KEY", None) if _local_cfg else None
        )
        aws_session_token = os.environ.get("AWS_SESSION_TOKEN") or (
            getattr(_local_cfg, "AWS_SESSION_TOKEN", None) if _local_cfg else None
        )

        if aws_access_key_id and aws_secret_access_key:
            self.dynamodb = boto3.resource(
                "dynamodb",
                region_name=self.region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token or None,
            )
        else:
            self.dynamodb = boto3.resource("dynamodb", region_name=self.region)

        # Required tables by prompt (lowercase defaults as requested)
        self.primary_users_table = os.environ.get("PRIMARY_USERS_TABLE", "primary_users")
        self.products_table = os.environ.get("PRODUCTS_TABLE", "products")
        self.lease_requests_table = os.environ.get("LEASE_REQUESTS_TABLE", "lease_requests")
        self.users_table = os.environ.get("USERS_TABLE", "users")
        self.manufacturer_contacts_table = os.environ.get("MANUFACTURER_CONTACTS_TABLE", "ev_manufacturer_contacts")

    # ------------------------------------------------------------------
    # Table management
    # ------------------------------------------------------------------
    def ensure_tables_and_seed(self):
        self._ensure_table(self.primary_users_table, "PID")
        self._ensure_table(self.products_table, "Product ID")
        self._ensure_table(self.lease_requests_table, "LeaseRequestID")
        self._ensure_table(self.users_table, "UserKey")

        if not self._table_has_items(self.products_table):
            self._seed_products()

    def _ensure_table(self, table_name, pk_name):
        try:
            table = self.dynamodb.Table(table_name)
            table.load()
            return
        except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
            pass

        table = self.dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": pk_name, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": pk_name, "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName=table_name)

    def _table_has_items(self, table_name):
        table = self.dynamodb.Table(table_name)
        try:
            resp = table.scan(Limit=1)
            return bool(resp.get("Items"))
        except ClientError:
            return False

    # ------------------------------------------------------------------
    # Product seeding + access
    # ------------------------------------------------------------------
    def _seed_products(self):
        products_df = pd.read_csv("ev_charger_marketplace_dataset.csv")
        table = self.dynamodb.Table(self.products_table)

        with table.batch_writer() as batch:
            for _, row in products_df.iterrows():
                item = {k: _to_decimal_safe(v) for k, v in row.to_dict().items()}
                batch.put_item(Item=item)

    def list_products(self):
        table = self.dynamodb.Table(self.products_table)
        resp = table.scan()
        return resp.get("Items", [])

    def get_product_plan_price(self, product: Dict, plan_selected: str) -> Optional[str]:
        """
        Resolve selected lease plan price from product row dynamically.
        Supports schema variants in source CSV/table.
        """
        plan = (plan_selected or "").strip().lower()
        if not product or not plan:
            return None

        if "2" in plan:
            keys = ["2 Month Lease (INR)", "2 Month Lease", "2_month_lease_inr", "2_month_lease"]
        elif "6" in plan:
            keys = ["6 Month Lease (INR)", "6 Month Lease", "6_month_lease_inr", "6_month_lease"]
        elif "12" in plan:
            keys = ["12 Month Lease (INR)", "12 Month Lease", "12_month_lease_inr", "12_month_lease"]
        else:
            return None

        for key in keys:
            if key in product and product.get(key) not in (None, ""):
                return str(product.get(key))
        return None

    def get_product_by_id(self, product_id: str):
        table = self.dynamodb.Table(self.products_table)
        resp = table.get_item(Key={"Product ID": product_id})
        return resp.get("Item")

    def get_manufacturer_contact_by_product_id(self, product_id: str) -> Optional[Dict]:
        """
        Fetch manufacturer contact row from ev_manufacturer_contacts by product id.
        Uses scan with flexible key matching to support schema variations.
        """
        if not product_id:
            return None

        table = self.dynamodb.Table(self.manufacturer_contacts_table)
        try:
            resp = table.scan()
        except ClientError:
            return None

        items = resp.get("Items", [])
        needle = str(product_id).strip().lower()
        product_id_keys = ["ProductID", "product_id", "Product ID", "productid"]

        for item in items:
            for key in product_id_keys:
                if key in item and str(item.get(key, "")).strip().lower() == needle:
                    return item
        return None

    # ------------------------------------------------------------------
    # Primary users
    # ------------------------------------------------------------------
    def create_primary_user(self, item: Dict):
        table = self.dynamodb.Table(self.primary_users_table)
        table.put_item(Item=item)

    def get_primary_user_by_pid(self, pid: str) -> Optional[Dict]:
        table = self.dynamodb.Table(self.primary_users_table)
        try:
            resp = table.get_item(Key={"PID": pid})
            return resp.get("Item")
        except ClientError:
            return None

    def find_primary_user_for_login(self, user_id_or_email: str) -> Optional[Dict]:
        table = self.dynamodb.Table(self.primary_users_table)
        resp = table.scan()
        items = resp.get("Items", [])
        needle = (user_id_or_email or "").strip().lower()

        for item in items:
            user_id = str(item.get("UserID", "")).strip().lower()
            email = str(item.get("Email", "")).strip().lower()
            if needle in (user_id, email):
                return item
        return None

    # ------------------------------------------------------------------
    # Lease requests
    # ------------------------------------------------------------------
    def create_lease_request(self, item: Dict):
        table = self.dynamodb.Table(self.lease_requests_table)
        table.put_item(Item=item)

    def list_lease_requests_by_pid(self, pid: str) -> List[Dict]:
        table = self.dynamodb.Table(self.lease_requests_table)
        resp = table.scan()
        items = resp.get("Items", [])
        return [x for x in items if x.get("PID") == pid]

    def update_lease_request_status(self, request_id: str, status: str):
        table = self.dynamodb.Table(self.lease_requests_table)
        table.update_item(
            Key={"LeaseRequestID": request_id},
            UpdateExpression="SET #S = :s",
            ExpressionAttributeNames={"#S": "Status"},
            ExpressionAttributeValues={":s": status},
        )

    def upsert_user_login_audit(self, name: str, email: str, role: str):
        """
        Upsert login details into lowercase users table.
        """
        table = self.dynamodb.Table(self.users_table)
        user_key = (email or "").strip().lower() or (name or "").strip().lower()
        if not user_key:
            user_key = "unknown-user"

        table.put_item(
            Item={
                "UserKey": user_key,
                "Name": name or "",
                "Email": (email or "").strip().lower(),
                "Role": role or "",
                "LoginTimestamp": str(pd.Timestamp.utcnow()),
            }
        )

    # ------------------------------------------------------------------
    # Utility builders
    # ------------------------------------------------------------------
    @staticmethod
    def build_primary_user_item(
        name: str,
        user_id: str,
        email: str,
        password_hash: str,
        address: str,
        install_location: str,
    ) -> Dict:
        pid = "PID-" + uuid.uuid4().hex[:10].upper()
        return {
            "PID": pid,
            "Name": name,
            "UserID": user_id,
            "Email": email,
            "Password": password_hash,
            "Address": address,
            "InstallLocation": install_location,
        }

    @staticmethod
    def build_lease_request_item(
        pid: str,
        product_id: str,
        product_name: str,
        plan_selected: str,
        installation_location: str,
        affidavit_file_path: str,
        payment_status: str = "Paid",
        status: str = "Request Sent",
        charger_type: str = "",
        lease_price: str = "",
        document_upload_status: str = "Uploaded",
    ) -> Dict:
        request_id = "LR-" + uuid.uuid4().hex[:12].upper()
        return {
            "LeaseRequestID": request_id,
            "PID": pid,
            "ProductID": product_id,
            "ProductName": product_name,
            "ChargerType": charger_type,
            "PlanSelected": plan_selected,
            "LeasePrice": lease_price,
            "InstallationLocation": installation_location,
            "AffidavitFilePath": affidavit_file_path,
            "DocumentUploadStatus": document_upload_status,
            "PaymentStatus": payment_status,
            "SubmissionTimestamp": str(pd.Timestamp.utcnow()),
            "Status": status,
        }
