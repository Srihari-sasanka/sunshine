"""
Email sending helper.
Tries AWS SES first, falls back to SMTP if configured.
Environment variables:
- USE_SES (optional, default true)
- SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS (for SMTP fallback)
- SENDER_EMAIL (required)
"""

import os
import smtplib
from email.message import EmailMessage

import boto3
from botocore.exceptions import ClientError

try:
    import aws_config as _local_cfg
except Exception:
    _local_cfg = None


class EmailService:
    def __init__(self):
        self.sender = os.environ.get("SENDER_EMAIL") or (
            getattr(_local_cfg, "SENDER_EMAIL", None) if _local_cfg else None
        ) or "no-reply@sunshineservices.com"

        self.region = os.environ.get("AWS_REGION") or (
            getattr(_local_cfg, "AWS_REGION", None) if _local_cfg else None
        ) or "us-east-1"

        use_ses_raw = os.environ.get("USE_SES")
        if use_ses_raw is None and _local_cfg is not None:
            use_ses_raw = str(getattr(_local_cfg, "USE_SES", "true"))
        self.use_ses = str(use_ses_raw if use_ses_raw is not None else "true").lower() != "false"

        self.ops_email = os.environ.get("OPS_EMAIL", "kandukuriv3@gmail.com")

        if self.use_ses:
            self.ses = boto3.client("ses", region_name=self.region)

        self.smtp_host = os.environ.get("SMTP_HOST") or (
            getattr(_local_cfg, "SMTP_HOST", None) if _local_cfg else None
        )

        smtp_port_raw = os.environ.get("SMTP_PORT")
        if smtp_port_raw is None and _local_cfg is not None:
            smtp_port_raw = getattr(_local_cfg, "SMTP_PORT", None)
        self.smtp_port = int(smtp_port_raw) if smtp_port_raw else None

        self.smtp_user = os.environ.get("SMTP_USER") or (
            getattr(_local_cfg, "SMTP_USER", None) if _local_cfg else None
        )
        self.smtp_pass = os.environ.get("SMTP_PASS") or (
            getattr(_local_cfg, "SMTP_PASS", None) if _local_cfg else None
        )

    def send_email(self, to_address, subject, body_text, body_html=None):
        if self.use_ses:
            try:
                return self.ses.send_email(
                    Source=self.sender,
                    Destination={"ToAddresses": [to_address]},
                    Message={
                        "Subject": {"Data": subject},
                        "Body": {
                            "Text": {"Data": body_text},
                            **({"Html": {"Data": body_html}} if body_html else {}),
                        },
                    },
                )
            except ClientError as e:
                print("SES send failed:", e)

        if self.smtp_host and self.smtp_port and self.smtp_user and self.smtp_pass:
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = self.sender
            msg["To"] = to_address
            msg.set_content(body_text)
            if body_html:
                msg.add_alternative(body_html, subtype="html")

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_pass)
                server.send_message(msg)
            return True

        raise Exception("No email delivery method configured (SES failed and SMTP not configured).")

    def send_manufacturer_notification(
        self,
        manufacturer_email,
        manufacturer_name,
        primary_user_name,
        product_selected,
        installation_address,
        plan_selected,
        random_mobile,
    ):
        subject = f"New EV Charger Lease Request - {product_selected}"
        body = (
            f"Hello {manufacturer_name},\n\n"
            f"A new lease request has been submitted on Sunshine Services.\n\n"
            f"Primary User Name: {primary_user_name}\n"
            f"Product Selected: {product_selected}\n"
            f"Installation Address: {installation_address}\n"
            f"Plan Selected: {plan_selected}\n"
            f"Primary Contact Number (generated): {random_mobile}\n\n"
            "Please contact the user to discuss installation.\n\n"
            "Regards,\nSunshine Services"
        )
        return self.send_email(manufacturer_email, subject, body)

    def send_primary_user_confirmation(
        self,
        user_email,
        user_name,
        product_selected,
        plan_selected,
        installation_location,
        price_paid=None,
        product_id=None,
    ):
        subject = "Sunshine EV Lease Request Submitted"
        body = (
            f"Dear {user_name},\n\n"
            "Your lease request has been successfully submitted.\n\n"
            "Product Details:\n"
            f"Product ID: {product_id or ''}\n"
            f"Product Name: {product_selected}\n"
            f"Lease Plan: {plan_selected}\n"
            f"Price Paid: {price_paid or ''}\n"
            f"Installation Location: {installation_location}\n\n"
            "Our team will review your documents and contact you shortly.\n\n"
            "Thank you for choosing Sunshine Services.\n\n"
            "Charge Up, Go Far."
        )
        return self.send_email(user_email, subject, body)

    def send_ops_notification(
        self,
        user_name,
        user_email,
        product_selected,
        plan_selected,
        amount,
    ):
        subject = "New EV Lease Request Received"
        body = (
            "A new lease request has been submitted.\n\n"
            f"User: {user_name}\n"
            f"Email: {user_email}\n"
            f"Product: {product_selected}\n"
            f"Lease Plan: {plan_selected}\n"
            f"Price: {amount}\n"
        )
        return self.send_email(self.ops_email, subject, body)
