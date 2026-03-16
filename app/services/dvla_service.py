"""
DVLA Vehicle Enquiry Service (VES) integration.

Free GOV.UK API. Requires DVLA_API_KEY environment variable.
Returns supplementary vehicle data not available from DVSA/MOT:
- Tax status (Taxed / Untaxed / SORN)
- CO2 emissions (accurate road tax calculation)
- Euro status (accurate ULEZ compliance check)
- Date of last V5C issue (keeper change indicator)
- Whether marked for export (red flag)

Register for free at:
https://developer-portal.driver-vehicle-licensing.api.gov.uk/
Set DVLA_API_KEY in your environment/Render config.
"""

import os
import requests

DVLA_API_KEY = os.getenv("DVLA_APP_KEY", os.getenv("DVLA_API_KEY", ""))
DVLA_VES_URL = "https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles"
DVLA_TIMEOUT = 5  # seconds


def get_dvla_vehicle_data(reg: str) -> dict:
    """
    Fetch supplementary vehicle data from DVLA VES.
    Returns empty dict if API key not set, reg missing, or request fails.
    Never raises — all errors are swallowed to avoid blocking deal processing.
    """
    if not DVLA_API_KEY or not reg:
        return {}

    clean_reg = reg.upper().replace(" ", "")

    try:
        response = requests.post(
            DVLA_VES_URL,
            json={"registrationNumber": clean_reg},
            headers={
                "x-api-key": DVLA_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=DVLA_TIMEOUT,
        )

        if response.status_code == 200:
            data = response.json()

            # Euro status → ULEZ compliance
            euro = data.get("euroStatus", "") or ""
            is_ulez_compliant = None
            if euro:
                try:
                    euro_num = int("".join(filter(str.isdigit, euro)))
                    is_ulez_compliant = euro_num >= 6
                except (ValueError, TypeError):
                    pass

            # V5C issue date — recent issue may indicate keeper change
            v5c_date = data.get("dateOfLastV5CIssued")

            return {
                "tax_status": data.get("taxStatus"),           # "Taxed" | "Untaxed" | "SORN"
                "tax_due_date": data.get("taxDueDate"),
                "mot_expiry": data.get("motExpiryDate"),
                "co2_emissions": data.get("co2Emissions"),     # g/km integer
                "euro_status": euro or None,
                "is_ulez_compliant": is_ulez_compliant,        # True/False/None
                "marked_for_export": data.get("markedForExport", False),
                "colour": data.get("colour"),
                "year_of_manufacture": data.get("yearOfManufacture"),
                "engine_capacity_cc": data.get("engineCapacity"),
                "fuel_type": data.get("fuelType"),
                "date_of_last_v5c": v5c_date,
                "wheelplan": data.get("wheelplan"),
            }

        elif response.status_code == 404:
            print(f"   ℹ️ DVLA VES: no record for {clean_reg}")
            return {}
        else:
            print(f"   ⚠️ DVLA VES: HTTP {response.status_code} for {clean_reg}")
            return {}

    except requests.Timeout:
        print(f"   ⚠️ DVLA VES timeout for {clean_reg}")
        return {}
    except Exception as e:
        print(f"   ⚠️ DVLA VES error for {clean_reg}: {e}")
        return {}


def is_sorn(dvla_data: dict) -> bool:
    return (dvla_data.get("tax_status") or "").upper() == "SORN"


def is_marked_for_export(dvla_data: dict) -> bool:
    return bool(dvla_data.get("marked_for_export"))


def get_annual_road_tax_from_co2(co2_gkm: int, fuel_type: str, year: int) -> int:
    """
    Estimate annual VED (road tax) from CO2 emissions for post-2001 vehicles.
    Uses simplified UK VED bands. For exact figures see DVLA VED tables.
    """
    if not co2_gkm:
        return None

    ft = (fuel_type or "").lower()

    # Post-April 2017: standard rate for most cars regardless of CO2 (except first year)
    if year and year >= 2017:
        if "electric" in ft or "ev" in ft:
            return 0
        return 180  # Standard rate (2024/25)

    # 2001-2017: CO2 banded
    if "electric" in ft or "ev" in ft:
        return 0
    if co2_gkm <= 100:
        return 0
    elif co2_gkm <= 110:
        return 20
    elif co2_gkm <= 120:
        return 30
    elif co2_gkm <= 130:
        return 130
    elif co2_gkm <= 140:
        return 165
    elif co2_gkm <= 150:
        return 180
    elif co2_gkm <= 165:
        return 220
    elif co2_gkm <= 175:
        return 265
    elif co2_gkm <= 185:
        return 290
    elif co2_gkm <= 200:
        return 330
    elif co2_gkm <= 225:
        return 370
    elif co2_gkm <= 255:
        return 600
    else:
        return 695
