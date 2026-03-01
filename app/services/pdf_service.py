from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
)
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import A4
from io import BytesIO


def generate_deal_pdf(deal, mot_data):

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=40,
        leftMargin=40,
        topMargin=40,
        bottomMargin=40,
    )

    elements = []
    styles = getSampleStyleSheet()

    gold = colors.HexColor("#C9A227")
    dark = colors.HexColor("#1A1A1A")

    # -----------------------------
    # CUSTOM STYLES
    # -----------------------------
    title_style = ParagraphStyle(
        "TitleStyle",
        parent=styles["Heading1"],
        fontSize=24,
        textColor=gold,
        spaceAfter=14,
    )

    section_style = ParagraphStyle(
        "SectionStyle",
        parent=styles["Heading2"],
        fontSize=16,
        textColor=gold,
        spaceBefore=10,
        spaceAfter=6,
    )

    normal_style = ParagraphStyle(
        "NormalStyle",
        parent=styles["Normal"],
        fontSize=11,
        spaceAfter=6,
    )

    small_style = ParagraphStyle(
        "SmallStyle",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.grey,
    )

    # -----------------------------
    # HEADER
    # -----------------------------
    elements.append(Paragraph("VehicleIntel Acquisition Report", title_style))
    elements.append(Spacer(1, 0.2 * inch))

    confidence_color = {
        "very_high": colors.green,
        "high": colors.darkgreen,
        "medium": colors.orange,
        "low": colors.red,
    }.get(deal.status, colors.black)

    elements.append(
        Paragraph(
            f"<b>Confidence Level:</b> "
            f"<font color='{confidence_color}'>"
            f"{deal.status.upper()}</font>",
            normal_style,
        )
    )

    elements.append(Spacer(1, 0.2 * inch))
    elements.append(HRFlowable(width="100%", thickness=1, color=gold))
    elements.append(Spacer(1, 0.3 * inch))

    report = deal.report or {}

    # -----------------------------
    # KEY METRICS SNAPSHOT
    # -----------------------------
    elements.append(Paragraph("Executive Summary", section_style))

    snapshot_data = [
        ["Listing Price", f"£{deal.listing_price}"],
        ["Market Value", f"£{deal.market_value}"],
        ["Estimated Profit", f"£{deal.profit}"],
        ["Risk Penalty", f"£{deal.risk_penalty}"],
        ["Score", f"{deal.score}"],
    ]

    snapshot_table = Table(snapshot_data, colWidths=[2.5 * inch, 2.5 * inch])
    snapshot_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.whitesmoke),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))

    elements.append(snapshot_table)
    elements.append(Spacer(1, 0.4 * inch))

    # -----------------------------
    # LISTING DETAILS
    # -----------------------------
    elements.append(Paragraph("Listing Details", section_style))

    listing_data = [
        ["Title", deal.title],
        ["Registration", deal.reg or "Not Found"],
        ["Mileage", str(deal.mileage)],
        ["Source", deal.source],
        ["Seller", report.get("seller", "N/A")],
        ["Location", report.get("location", "N/A")],
        ["Listing URL", report.get("listing_url", "N/A")],
    ]

    listing_table = Table(listing_data, colWidths=[2.2 * inch, 3.3 * inch])
    listing_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.whitesmoke),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))

    elements.append(listing_table)
    elements.append(Spacer(1, 0.4 * inch))

    # -----------------------------
    # CAP VALUATION BREAKDOWN
    # -----------------------------
    elements.append(Paragraph("Valuation Intelligence (CAP)", section_style))

    cap_data = report.get("cap_data", {})

    if cap_data:
        cap_rows = [["Metric", "Value"]]
        for k, v in cap_data.items():
            cap_rows.append([k.replace("_", " ").title(), f"£{v}"])
    else:
        cap_rows = [["CAP Data", "Not Available"]]

    cap_table = Table(cap_rows, colWidths=[2.5 * inch, 2.5 * inch])
    cap_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), gold),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))

    elements.append(cap_table)
    elements.append(Spacer(1, 0.4 * inch))

    # -----------------------------
    # MOT HISTORY
    # -----------------------------
    elements.append(Paragraph("MOT History Analysis", section_style))

    mot_rows = [["Date", "Result", "Notes"]]
    fail_count = 0
    advisory_count = 0

    if mot_data:
        try:
            tests = mot_data[0].get("motTests", [])
            for test in tests[:6]:

                result = test.get("testResult", "")
                date = test.get("completedDate", "")

                comments = test.get("rfrAndComments", [])
                notes = "; ".join(c.get("text", "") for c in comments)

                if result == "FAIL":
                    fail_count += 1

                advisory_count += len(comments)

                mot_rows.append([date, result, notes])

        except Exception:
            mot_rows.append(["Error reading MOT data", "", ""])
    else:
        mot_rows.append(["No MOT data available", "", ""])

    mot_table = Table(mot_rows, colWidths=[1.3 * inch, 1 * inch, 3.2 * inch])
    mot_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), gold),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))

    elements.append(mot_table)
    elements.append(Spacer(1, 0.3 * inch))

    # MOT STATS
    elements.append(
        Paragraph(
            f"<b>Total Recorded Fails:</b> {fail_count} | "
            f"<b>Total Advisory Items:</b> {advisory_count}",
            normal_style,
        )
    )

    elements.append(Spacer(1, 0.4 * inch))

    # -----------------------------
    # FINAL RECOMMENDATION
    # -----------------------------
    elements.append(HRFlowable(width="100%", thickness=1, color=gold))
    elements.append(Spacer(1, 0.3 * inch))

    recommendation = f"""
    Based on valuation intelligence, risk modelling and MOT analysis,
    this vehicle is classified as <b>{deal.status.upper()}</b> confidence.

    Estimated Risk-Adjusted Profit: <b>£{deal.profit}</b>  
    Risk-Adjusted Score: <b>{deal.score}</b>
    """

    elements.append(Paragraph(recommendation, normal_style))

    elements.append(Spacer(1, 0.4 * inch))
    elements.append(
        Paragraph(
            "Generated by VehicleIntel – Acquisition Intelligence Engine",
            small_style,
        )
    )

    doc.build(elements)
    buffer.seek(0)

    return buffer