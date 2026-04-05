"""
Test data generator for Insurance Claims dataset

Creates realistic test data for local development and testing
"""
import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal
import random
from faker import Faker
import logging

logger = logging.getLogger(__name__)


class InsuranceDataGenerator:
    """
    Generate realistic insurance claims test data in CSV and JSON formats.
    """

    def __init__(self, num_records: int = 1000, seed: int = 42):
        """
        Initialize data generator.

        Args:
            num_records: Number of claim records to generate
            seed: Random seed for reproducibility
        """
        self.num_records = num_records
        self.seed = seed

        # Set seeds for reproducibility
        self.faker = Faker()
        Faker.seed(seed)
        random.seed(seed)

        logger.info(
            f"Initialized generator with seed={seed}, num_records={num_records}")

    def generate_claims_data(self) -> pd.DataFrame:
        """
        Generate claims dataset.

        Returns:
            DataFrame with columns:
            - claim_id: Unique claim identifier
            - policy_id: Associated policy
            - customer_name: Customer name
            - claim_type: Type of claim (auto, home, health, life, disability)
            - claim_amount: Claim amount in dollars
            - description: Claim description
            - submitted_date: When claim was submitted
            - resolution_date: When claim was resolved (or null if pending)
            - status: Current status (pending, approved, denied, under_review)
        """
        claims = []

        for i in range(self.num_records):
            submitted_date = datetime.now() - timedelta(days=random.randint(1, 365))
            resolution_days = random.randint(
                1, 180) if random.random() > 0.2 else None

            resolution_date = (
                submitted_date + timedelta(days=resolution_days)
                if resolution_days
                else None
            )

            status = random.choices(
                ["pending", "approved", "denied", "under_review"],
                weights=[0.1, 0.6, 0.2, 0.1]
            )[0]

            claims.append({
                "claim_id": f"CLM-{i + 1:06d}",
                "policy_id": f"POL-{random.randint(10000, 99999):05d}",
                "customer_name": self.faker.name(),
                "claim_type": random.choice(["auto", "home", "health", "life", "disability"]),
                "claim_amount": round(random.uniform(100, 50000), 2),
                "description": self.faker.sentence(),
                "submitted_date": submitted_date.strftime("%Y-%m-%d"),
                "resolution_date": resolution_date.strftime("%Y-%m-%d") if resolution_date else None,
                "status": status,
            })

        return pd.DataFrame(claims)

    def generate_customers_data(self) -> pd.DataFrame:
        """
        Generate customer dimension data.

        Returns:
            DataFrame with columns:
            - customer_id: Unique customer ID
            - policy_id: Associated policy
            - customer_name: Full name
            - customer_email: Email
            - customer_phone: Phone number
            - customer_ssn: Social Security Number (PII - will be masked later)
            - customer_income: Annual income
            - customer_address: Address (PII - will be masked later)
            - signup_date: When customer signed up
        """
        customers = []

        num_customers = max(100, self.num_records // 10)

        for i in range(num_customers):
            customers.append({
                "customer_id": f"CUST-{i + 1:06d}",
                "policy_id": f"POL-{random.randint(10000, 99999):05d}",
                "customer_name": self.faker.name(),
                "customer_email": self.faker.email(),
                "customer_phone": self.faker.phone_number(),
                "customer_ssn": self.faker.ssn(),  # PII - will mask this later
                "customer_income": round(random.uniform(30000, 200000), 2),
                "customer_address": self.faker.address(),  # PII - will mask this later
                "signup_date": (
                    datetime.now() - timedelta(days=random.randint(365, 1825))
                ).strftime("%Y-%m-%d"),
            })

        return pd.DataFrame(customers)

    def generate_claim_notes_data(self) -> pd.DataFrame:
        """
        Generate claim adjuster notes (semi-structured data).

        These are adjuster observations about claims, including 
        potential fraud indicators.

        Returns:
            DataFrame with columns:
            - claim_id: Associated claim
            - note_id: Unique note identifier
            - adjuster_name: Adjuster who wrote note
            - note_text: Adjuster comment/observation
            - note_date: When note was written
            - fraud_indicator: Y/N/UNKNOWN
            - severity_level: 1-5 scale
        """
        notes = []
        note_id_counter = 1

        # Each claim can have 0-3 notes
        num_claims = min(self.num_records, 1000)

        for claim_idx in range(num_claims):
            num_notes = random.choices([0, 1, 2, 3], weights=[
                                       0.3, 0.4, 0.2, 0.1])[0]

            for _ in range(num_notes):
                note_date = datetime.now() - timedelta(days=random.randint(1, 365))

                notes.append({
                    "claim_id": f"CLM-{claim_idx + 1:06d}",
                    "note_id": f"NOTE-{note_id_counter:08d}",
                    "adjuster_name": self.faker.name(),
                    "note_text": self.faker.sentence(nb_words=10),
                    "note_date": note_date.strftime("%Y-%m-%d"),
                    "fraud_indicator": random.choice(["Y", "N", "UNKNOWN"]),
                    "severity_level": random.randint(1, 5),
                })
                note_id_counter += 1

        return pd.DataFrame(notes)

    def save_claims_csv(self, output_path: Path) -> Path:
        """Save claims data as CSV"""
        output_path.mkdir(parents=True, exist_ok=True)
        csv_file = output_path / "claims.csv"

        df = self.generate_claims_data()
        df.to_csv(csv_file, index=False)

        logger.info(
            f"Generated CSV with {len(df)} claim records at {csv_file}")
        return csv_file

    def save_customers_csv(self, output_path: Path) -> Path:
        """Save customers data as CSV"""
        output_path.mkdir(parents=True, exist_ok=True)
        csv_file = output_path / "customers.csv"

        df = self.generate_customers_data()
        df.to_csv(csv_file, index=False)

        logger.info(
            f"Generated CSV with {len(df)} customer records at {csv_file}")
        return csv_file

    def save_claim_notes_json(self, output_path: Path) -> Path:
        """Save claim notes as JSON (semi-structured)"""
        output_path.mkdir(parents=True, exist_ok=True)
        json_file = output_path / "claim_notes.json"

        df = self.generate_claim_notes_data()
        records = df.to_dict('records')

        with open(json_file, 'w') as f:
            json.dump(records, f, indent=2)

        logger.info(
            f"Generated JSON with {len(records)} claim notes at {json_file}")
        return json_file

    def generate_all(self, output_path: Path) -> dict:
        """
        Generate all datasets and save to disk.

        Returns:
            Dict with paths to created files
        """
        output_path.mkdir(parents=True, exist_ok=True)

        paths = {
            "claims_csv": self.save_claims_csv(output_path),
            "customers_csv": self.save_customers_csv(output_path),
            "claim_notes_json": self.save_claim_notes_json(output_path),
        }

        logger.info(f"Generated all test data in {output_path}")
        return paths


# Convenience function for quick data generation
def generate_test_data(output_dir: str, num_records: int = 1000) -> dict:
    """
    Quick function to generate test data.

    Usage:
        from src.utils.data_generator import generate_test_data
        paths = generate_test_data("data/bronze", num_records=5000)
    """
    generator = InsuranceDataGenerator(num_records=num_records)
    return generator.generate_all(Path(output_dir))


if __name__ == "__main__":
    # If run directly, generate sample data
    generator = InsuranceDataGenerator(num_records=1000)
    output = generator.generate_all(Path("data/bronze"))
    print("Generated test data:")
    for name, path in output.items():
        print(f"  {name}: {path}")
