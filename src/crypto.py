#  filename: crypto.py
import datetime
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from cryptography import x509
from cryptography.hazmat.primitives import hashes


def generate_ssc():
    """generate self signed certificate"""
    # Generate private key
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    with open("key.pem", "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.BestAvailableEncryption(b"portport") # is this a security issue?
        ))
    # generate dummy self-signed certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "ZZ"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "ZZ"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "ZZ"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ZZ"),
        x509.NameAttribute(NameOID.COMMON_NAME, "portport")
    ])

    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.now(datetime.timezone.utc)
    ).not_valid_after(
        datetime.datetime.now() + datetime.timedelta(days=15)
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName("localhost")]),
        critical=False
    ).sign(key, hashes.SHA256())

    with open("cert.pem", "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    



    