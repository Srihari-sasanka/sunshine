"""Sunshine Services EV Charger Lease Management Platform - Flask backend
Run: python app.py
"""

import os
import random
from datetime import datetime

import pandas as pd
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

from database import Database
from email_service import EmailService

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
UPLOAD_FOLDER = os.path.join("static", "uploads")
ALLOWED_EXTENSIONS = {"pdf", "jpg", "jpeg", "png", "doc", "docx"}

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10MB

CORS(app)

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

db = Database(region_name=AWS_REGION)
db.ensure_tables_and_seed()
emailer = EmailService()


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def normalize_product_record(item):
    def pick(keys, default=""):
        for k in keys:
            if k in item and item.get(k) not in (None, ""):
                return item.get(k)
        return default

    return {
        "product_id": pick(["Product ID", "product_id", "ProductID"], None),
        "brand": pick(["Brand", "brand"], None),
        "product_name": pick(["Product Name", "product_name", "ProductName"], None),
        "charger_type": pick(["Charger Type", "charger_type", "ChargerType"], None),
        "power_output": pick(["Power Output", "power_output", "PowerOutput"], None),
        "suitable_vehicles": pick(["Suitable Vehicles", "suitable_vehicles", "SuitableVehicles"], None),
        "description": pick(["Description", "description"], None),
        "lease_6_month": str(pick(["6 Month Lease (INR)", "6_month_lease_inr", "6 Month Lease"], "")),
        "lease_12_month": str(pick(["12 Month Lease (INR)", "12_month_lease_inr", "12 Month Lease"], "")),
        "lease_24_month": str(pick(["24 Month Lease (INR)", "24_month_lease_inr", "24 Month Lease", "12 Month Lease (INR)"], "")),
    }


def load_manufacturer_contacts():
    try:
        df = pd.read_excel("ev_manufacturer_contacts.xlsx")
        records = []
        for _, row in df.iterrows():
            normalized = {str(k).strip().lower(): str(v).strip() for k, v in row.to_dict().items()}
            records.append(normalized)
        return records
    except Exception as e:
        app.logger.warning(f"Unable to load manufacturer contacts xlsx: {e}")
        return []


def _extract_email_from_row(values):
    if not values:
        return None
    if not isinstance(values, dict):
        return None
    lowered = {str(k).strip().lower(): str(v).strip() for k, v in values.items()}
    email_candidates = [
        lowered.get("email"),
        lowered.get("email id"),
        lowered.get("email_id"),
        lowered.get("contact email"),
        lowered.get("contact_email"),
        lowered.get("mail"),
    ]
    for em in email_candidates:
        if em and "@" in em:
            return em
    return None


def resolve_manufacturer_email(manufacturer_name):
    contacts = load_manufacturer_contacts()
    needle = (manufacturer_name or "").strip().lower()

    for c in contacts:
        values = {k.lower(): v for k, v in c.items()}
        name_candidates = [
            values.get("manufacturer"),
            values.get("manufacturer name"),
            values.get("brand"),
            values.get("company"),
            values.get("name"),
        ]
        if any(n and needle in n.lower() for n in name_candidates):
            em = _extract_email_from_row(values)
            if em:
                return em
    # fallback
    safe_name = needle.replace(" ", "")
    return f"{safe_name}@example.com" if safe_name else "manufacturer@example.com"


def resolve_manufacturer_email_by_product_id(product_id, manufacturer_name=None):
    row = db.get_manufacturer_contact_by_product_id(product_id)
    if row:
        row_email = _extract_email_from_row(row)
        if row_email:
            return row_email

    # fallback to existing manufacturer-name based resolver (xlsx)
    return resolve_manufacturer_email(manufacturer_name)


# -------------------------------------------------------------------
# Pages
# -------------------------------------------------------------------
@app.route("/")
def landing_page():
    return render_template("landing.html")


@app.route("/overview")
def overview_page():
    return render_template("overview.html")


@app.route("/services")
def services_page():
    return render_template("services.html")


@app.route("/login")
def login_page():
    return render_template("login.html")


@app.route("/primary-user")
def primary_user_page():
    return render_template("primary_auth.html")


@app.route("/register")
def register_page():
    return render_template("register.html")


@app.route("/primary-login")
def primary_login_page():
    return render_template("primary_login.html")


@app.route("/dashboard")
def dashboard_page():
    return render_template("dashboard.html")


@app.route("/payment")
def payment_page():
    return render_template("payment.html")


# -------------------------------------------------------------------
# APIs
# -------------------------------------------------------------------
@app.route("/api/products", methods=["GET"])
def api_products():
    products = db.list_products()
    normalized = [normalize_product_record(p) for p in products]

    dropdown_entries = []
    for p in normalized:
        product_id = p.get("product_id")
        product_name = p.get("product_name")
        charger_type = p.get("charger_type")

        plan_map = [
            ("2 Months", db.get_product_plan_price(products[normalized.index(p)], "2 Month") if products else None),
            ("6 Months", db.get_product_plan_price(products[normalized.index(p)], "6 Month") if products else None),
            ("12 Months", db.get_product_plan_price(products[normalized.index(p)], "12 Month") if products else None),
        ]

        for plan_label, price in plan_map:
            if price in (None, ""):
                continue
            dropdown_entries.append(
                {
                    "product_id": product_id,
                    "product_name": product_name,
                    "charger_type": charger_type,
                    "lease_plan": plan_label,
                    "price_inr": str(price),
                    "display": (
                        f"Product ID: {product_id} | Product Name: {product_name} | "
                        f"Charger Type: {charger_type} | Lease Plan: {plan_label} | Price: {price} INR"
                    ),
                }
            )

    return jsonify({"products": normalized, "dropdown_options": dropdown_entries})


@app.route("/api/register-primary", methods=["POST"])
def api_register_primary():
    data = request.get_json() or request.form

    name = (data.get("name") or "").strip()
    user_id = (data.get("user_id") or "").strip()
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    confirm_password = data.get("confirm_password") or ""
    address = (data.get("address") or "").strip()
    install_location = (data.get("install_location") or "").strip()

    if not all([name, user_id, email, password, confirm_password, address, install_location]):
        return jsonify({"error": "All fields are required."}), 400
    if password != confirm_password:
        return jsonify({"error": "Password and Confirm Password do not match."}), 400

    existing = db.find_primary_user_for_login(user_id) or db.find_primary_user_for_login(email)
    if existing:
        return jsonify({"error": "User ID or Email already exists."}), 409

    password_hash = generate_password_hash(password)
    item = db.build_primary_user_item(
        name=name,
        user_id=user_id,
        email=email,
        password_hash=password_hash,
        address=address,
        install_location=install_location,
    )
    db.create_primary_user(item)

    return jsonify({"message": "Account created successfully", "PID": item["PID"]}), 201


@app.route("/api/login-primary", methods=["POST"])
def api_login_primary():
    data = request.get_json() or request.form
    user_id_or_email = (data.get("user_id_or_email") or "").strip()
    password = data.get("password") or ""

    if not user_id_or_email or not password:
        return jsonify({"error": "User ID/Email and password are required."}), 400

    user = db.find_primary_user_for_login(user_id_or_email)
    if not user:
        return jsonify({"error": "Invalid credentials."}), 401

    if not check_password_hash(user.get("Password", ""), password):
        return jsonify({"error": "Invalid credentials."}), 401

    db.upsert_user_login_audit(
        name=user.get("Name", ""),
        email=user.get("Email", ""),
        role="Primary User",
    )

    return jsonify(
        {
            "message": "Login successful",
            "PID": user.get("PID"),
            "Name": user.get("Name"),
            "Email": user.get("Email"),
            "InstallLocation": user.get("InstallLocation"),
        }
    )


@app.route("/api/create-lease-request", methods=["POST"])
def api_create_lease_request():
    pid = (request.form.get("pid") or "").strip()
    product_id = (request.form.get("product_id") or "").strip()
    plan_selected = (request.form.get("plan_selected") or "").strip()
    installation_location = (request.form.get("installation_location") or "").strip()
    payment_status = (request.form.get("payment_status") or "Paid").strip()
    amount_paid = (request.form.get("amount_paid") or "").strip()

    affidavit = request.files.get("affidavit")

    if not all([pid, product_id, plan_selected, installation_location]):
        return jsonify({"error": "pid, product_id, plan_selected, installation_location are required."}), 400

    if not affidavit or affidavit.filename == "":
        return jsonify({"error": "Affidavit document is required."}), 400

    if not allowed_file(affidavit.filename):
        return jsonify({"error": "Unsupported file type for affidavit."}), 400

    product = db.get_product_by_id(product_id)
    if not product:
        return jsonify({"error": "Product not found."}), 404

    user = db.get_primary_user_by_pid(pid)
    if not user:
        return jsonify({"error": "Primary user not found."}), 404

    filename = secure_filename(f"{pid}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{affidavit.filename}")
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    affidavit.save(file_path)

    expected_price = db.get_product_plan_price(product, plan_selected)
    if not expected_price:
        return jsonify({"error": "Unable to resolve plan price from product data."}), 400

    if amount_paid and str(amount_paid).strip() != str(expected_price).strip():
        return jsonify(
            {
                "error": "Payment amount mismatch for selected plan.",
                "expected_amount": str(expected_price),
                "received_amount": str(amount_paid),
            }
        ), 400

    req_item = db.build_lease_request_item(
        pid=pid,
        product_id=product_id,
        product_name=product.get("Product Name", ""),
        plan_selected=plan_selected,
        installation_location=installation_location,
        affidavit_file_path=file_path.replace("\\", "/"),
        payment_status=payment_status,
        status="Request Sent",
        charger_type=str(product.get("Charger Type", "")),
        lease_price=str(expected_price),
        document_upload_status="Uploaded",
    )
    db.create_lease_request(req_item)

    manufacturer_name = product.get("Brand", "Manufacturer")
    manufacturer_email = resolve_manufacturer_email_by_product_id(product_id, manufacturer_name)
    random_mobile = "".join(str(random.randint(0, 9)) for _ in range(10))
    user_email = (user.get("Email", "") or "").strip()

    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            app.logger.info(
                f"Sending manufacturer email attempt={attempt} to={manufacturer_email} product_id={product_id}"
            )
            emailer.send_manufacturer_notification(
                manufacturer_email=manufacturer_email,
                manufacturer_name=manufacturer_name,
                primary_user_name=user.get("Name", "Primary User"),
                product_selected=product.get("Product Name", product_id),
                installation_address=user.get("Address", installation_location),
                plan_selected=plan_selected,
                random_mobile=random_mobile,
            )
            break
        except Exception as e:
            app.logger.warning(
                f"Manufacturer email failed attempt={attempt} to={manufacturer_email} "
                f"product_id={product_id} error={e}"
            )

    if not user_email:
        app.logger.warning(f"Primary user email missing for PID={pid}; skipping confirmation email")
    else:
        for attempt in range(1, max_retries + 1):
            try:
                app.logger.info(
                    f"Sending primary user email attempt={attempt} to={user_email} pid={pid} product_id={product_id}"
                )
                emailer.send_primary_user_confirmation(
                    user_email=user_email,
                    user_name=user.get("Name", "Primary User"),
                    product_selected=product.get("Product Name", product_id),
                    plan_selected=plan_selected,
                    installation_location=installation_location,
                    price_paid=str(expected_price),
                    product_id=product_id,
                )
                break
            except Exception as e:
                app.logger.warning(
                    f"Primary user email failed attempt={attempt} to={user_email} "
                    f"pid={pid} product_id={product_id} error={e}"
                )

    ops_email = os.environ.get("OPS_EMAIL", "kandukuriv3@gmail.com")
    for attempt in range(1, max_retries + 1):
        try:
            app.logger.info(
                f"Sending ops email attempt={attempt} to={ops_email} pid={pid} product_id={product_id}"
            )
            emailer.send_ops_notification(
                user_name=user.get("Name", "Primary User"),
                user_email=user_email,
                product_selected=product.get("Product Name", product_id),
                plan_selected=plan_selected,
                amount=str(expected_price),
            )
            break
        except Exception as e:
            app.logger.warning(
                f"Ops email failed attempt={attempt} to={ops_email} "
                f"pid={pid} product_id={product_id} error={e}"
            )

    return jsonify(
        {
            "message": "Lease request submitted successfully.",
            "LeaseRequestID": req_item["LeaseRequestID"],
            "Status": req_item["Status"],
        }
    ), 201


@app.route("/api/lease-requests/<pid>", methods=["GET"])
def api_list_lease_requests(pid):
    requests = db.list_lease_requests_by_pid(pid)
    return jsonify(requests)


@app.route("/api/update-request-status", methods=["POST"])
def api_update_request_status():
    data = request.get_json() or request.form
    request_id = (data.get("lease_request_id") or "").strip()
    status = (data.get("status") or "").strip()

    allowed = {"Request Sent", "Had Phone Conversation", "Installation Completed", "Request Rejected"}
    if status not in allowed:
        return jsonify({"error": "Invalid status value"}), 400
    if not request_id:
        return jsonify({"error": "lease_request_id is required"}), 400

    db.update_lease_request_status(request_id, status)
    return jsonify({"message": "Status updated"}), 200


@app.route("/api/simulate-payment", methods=["POST"])
def api_simulate_payment():
    data = request.get_json() or request.form
    pid = (data.get("pid") or "").strip()
    amount = (data.get("amount") or "").strip()
    if not pid:
        return jsonify({"error": "pid is required"}), 400
    tx_id = "TXN-" + "".join(str(random.randint(0, 9)) for _ in range(12))
    return jsonify({"message": "Payment successful", "transaction_id": tx_id, "amount": amount}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
