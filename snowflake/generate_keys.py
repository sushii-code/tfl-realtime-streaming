"""Create an RSA key pair for Snowflake and print the public key for the ALTER USER line."""

# Path makes working with file and folder paths easy
from pathlib import Path
# sys lets us stop the script early with a message
import sys

# serialization converts keys into the text formats that files use
from cryptography.hazmat.primitives import serialization
# rsa creates the key pair
from cryptography.hazmat.primitives.asymmetric import rsa

# Find the project's main folder: this file is in snowflake/, so go up two levels
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# The folder for the keys; .gitignore stops it from going to GitHub
KEYS_DIR = PROJECT_ROOT / "keys"
# The private key file, which must stay secret
PRIVATE_KEY_PATH = KEYS_DIR / "rsa_key.p8"
# The public key file, which is safe to share
PUBLIC_KEY_PATH = KEYS_DIR / "rsa_key.pub"


# Define the main function, which runs each step in order
def main():
    # Replacing existing keys would break a working login, so check for them first
    if PRIVATE_KEY_PATH.exists() or PUBLIC_KEY_PATH.exists():
        # Stop the script and show a clear message
        sys.exit(f"Keys already exist in {KEYS_DIR}, so nothing was changed.")
    # Create the keys folder if it does not exist yet
    KEYS_DIR.mkdir(exist_ok=True)
    # Create a 2048-bit RSA key pair, using the standard public exponent
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    # Convert the private key to text in PKCS8 format
    private_pem = private_key.private_bytes(
        # PEM is the text form of a key
        encoding=serialization.Encoding.PEM,
        # PKCS8 is the format Snowflake's connector reads
        format=serialization.PrivateFormat.PKCS8,
        # No passphrase, which is acceptable for this local project
        encryption_algorithm=serialization.NoEncryption(),
    # End of the conversion
    )
    # Convert the public key to text
    public_pem = private_key.public_key().public_bytes(
        # PEM is the text form of a key
        encoding=serialization.Encoding.PEM,
        # SubjectPublicKeyInfo is the standard format for a public key
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    # End of the conversion
    )
    # Open the private key file; "x" refuses to overwrite a file that already exists
    with PRIVATE_KEY_PATH.open("xb") as file:
        # Save the private key
        file.write(private_pem)
    # Open the public key file; "x" refuses to overwrite a file that already exists
    with PUBLIC_KEY_PATH.open("xb") as file:
        # Save the public key
        file.write(public_pem)
    # Remove the BEGIN and END lines, then join the rest into one line, as Snowflake expects
    public_key_text = "".join(public_pem.decode("ascii").splitlines()[1:-1])
    # Print only the public key, ready to paste into Snowsight
    print(f"ALTER USER TFL_LOADER SET RSA_PUBLIC_KEY='{public_key_text}';")


# Run main() only when this file is run directly, not when it is imported
if __name__ == "__main__":
    # Start the script
    main()
