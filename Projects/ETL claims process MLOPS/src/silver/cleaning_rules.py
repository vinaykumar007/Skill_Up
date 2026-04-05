"""
Cleaning rules and utilities for Silver layer

Reusable, testable functions for data transformation.
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class ClaimCleaningRules:
    """
    Reusable cleaning rules for insurance claim data.

    """

    @staticmethod
    def standardize_claim_amount(value: Any) -> float:
        """
        Convert claim amount to standardized decimal format.

        Handles:
        - String values with $ or commas
        - None/null values
        - Invalid numbers

        Returns:
            Float claim amount or 0.0 if invalid
        """
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return 0.0

        if isinstance(value, str):
            # Remove currency symbols and commas
            value = value.replace("$", "").replace(",", "").strip()
            if not value:
                return 0.0

        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert claim_amount: {value}")
            return 0.0

    @staticmethod
    def standardize_date(date_str: Any, format: str = "%Y-%m-%d") -> str:
        """
        Convert date to standard YYYY-MM-DD format.

        Handles multiple input formats.

        Returns:
            Standardized date string or None if invalid
        """
        if date_str is None or (isinstance(date_str, float) and np.isnan(date_str)):
            return None

        if isinstance(date_str, str):
            date_str = date_str.strip()
            if not date_str:
                return None

        try:
            # Try multiple common formats
            formats = [
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%m-%d-%Y",
                "%d/%m/%Y",
                "%Y%m%d",
            ]

            for fmt in formats:
                try:
                    parsed = pd.to_datetime(date_str, format=fmt)
                    return parsed.strftime("%Y-%m-%d")
                except:
                    continue

            # Fallback: try pandas parsing
            parsed = pd.to_datetime(date_str)
            return parsed.strftime("%Y-%m-%d")

        except Exception as e:
            logger.warning(
                f"Could not parse date: {date_str}, error: {str(e)}")
            return None

    @staticmethod
    def standardize_status(status: Any) -> str:
        """
        Normalized claim status to standard values.

        Valid values: pending, approved, denied, under_review
        """
        if status is None or (isinstance(status, float) and np.isnan(status)):
            return "unknown"

        status = str(status).lower().strip()

        # Map common variations
        status_mapping = {
            "pending": "pending",
            "pend": "pending",
            "approved": "approved",
            "approve": "approved",
            "approved_": "approved",
            "closed": "approved",
            "denied": "denied",
            "deny": "denied",
            "rejected": "denied",
            "rejected_review": "under_review",
            "under review": "under_review",
            "under_review": "under_review",
            "in_review": "under_review",
            "review": "under_review",
        }

        normalized = status_mapping.get(status, "unknown")
        return normalized

    @staticmethod
    def standardize_claim_type(claim_type: Any) -> str:
        """
        Normalize claim type to standard categories.

        Valid: auto, home, health, life, disability
        """
        if claim_type is None or (isinstance(claim_type, float) and np.isnan(claim_type)):
            return "unknown"

        claim_type = str(claim_type).lower().strip()

        # Map variations
        type_mapping = {
            "auto": "auto",
            "automobile": "auto",
            "vehicle": "auto",
            "car": "auto",
            "home": "home",
            "homeowner": "home",
            "house": "home",
            "property": "home",
            "health": "health",
            "medical": "health",
            "life": "life",
            "disability": "disability",
            "disab": "disability",
            "dis": "disability",
        }

        normalized = type_mapping.get(claim_type, "unknown")
        return normalized

    @staticmethod
    def categorize_claim_amount(amount: float) -> str:
        """
        Categorize claim amount for analysis.

        Returns: small, medium, large
        """
        if amount < 1000:
            return "small"
        elif amount < 10000:
            return "medium"
        else:
            return "large"

    @staticmethod
    def calculate_days_to_resolution(submitted_date: str, resolution_date: str) -> int:
        """
        Calculate days between submission and resolution.

        Returns:
            Days (integer) or None if either date is invalid
        """
        if submitted_date is None or resolution_date is None:
            return None

        try:
            submit = pd.to_datetime(submitted_date)
            resolve = pd.to_datetime(resolution_date)
            delta = resolve - submit
            return int(delta.days)
        except:
            return None

    @staticmethod
    def extract_date_parts(date_str: str) -> Dict[str, int]:
        """
        Extract year, month, day, quarter from date string.

        Returns:
            Dict with year, month, day, quarter, day_of_week
        """
        if date_str is None:
            return {
                "year": None,
                "month": None,
                "day": None,
                "quarter": None,
                "day_of_week": None,
            }

        try:
            dt = pd.to_datetime(date_str)
            return {
                "year": int(dt.year),
                "month": int(dt.month),
                "day": int(dt.day),
                "quarter": int((dt.month - 1) // 3 + 1),
                "day_of_week": int(dt.dayofweek),  # 0=Monday, 6=Sunday
            }
        except:
            return {
                "year": None,
                "month": None,
                "day": None,
                "quarter": None,
                "day_of_week": None,
            }


class CustomerCleaningRules:
    """
    Cleaning rules for customer dimension data.

    Includes PII masking for compliance.
    """

    @staticmethod
    def mask_ssn(ssn: Any) -> str:
        """
        Mask SSN for PII protection.
        """
        if ssn is None or (isinstance(ssn, float) and np.isnan(ssn)):
            return "MASKED"

        ssn = str(ssn).strip()
        if len(ssn) >= 4:
            return f"XXX-XX-{ssn[-4:]}"
        else:
            return "MASKED"

    @staticmethod
    def mask_address(address: Any) -> str:
        """
        Mask customer address for privacy.

        Keep postal code, mask street address.
        """
        if address is None or (isinstance(address, float) and np.isnan(address)):
            return "MASKED"

        address = str(address).strip()

        # Simple masking: keep only last word (often postal code/region)
        parts = address.split()
        if len(parts) > 0:
            return f"MASKED_{parts[-1]}"
        else:
            return "MASKED"

    @staticmethod
    def standardize_email(email: Any) -> str:
        """
        Standardize email (lowercase, validate format).
        """
        if email is None or (isinstance(email, float) and np.isnan(email)):
            return None

        email = str(email).lower().strip()

        # Basic email validation
        if "@" in email and "." in email:
            return email
        else:
            return None

    @staticmethod
    def standardize_phone(phone: Any) -> str:
        """
        Standardize phone number.

        Removes special characters, keeps only digits.
        """
        if phone is None or (isinstance(phone, float) and np.isnan(phone)):
            return None

        phone = str(phone).strip()

        # Keep only digits and +
        phone = "".join(c for c in phone if c.isdigit() or c == "+")

        if len(phone) >= 10:
            return phone
        else:
            return None
