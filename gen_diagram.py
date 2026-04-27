from PIL import Image, ImageDraw, ImageFont

W, H = 1400, 960
img = Image.new("RGB", (W, H), "#FFFFFF")
draw = ImageDraw.Draw(img)

try:
    font_title = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 22)
    font_h2 = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 13)
    font_b = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 12)
    font_s = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 11)
    font_xs = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 9)
except:
    font_title = font_b = font_h2 = font_s = font_xs = ImageFont.load_default()

BG       = "#FFFFFF"
BLUE     = "#29B5E8"
BLUE_D   = "#11749E"
BLUE_L   = "#E1F5FE"
BLUE_BOX = "#D4F0FC"
CYAN     = "#4DD0E1"
PURPLE   = "#7E57C2"
PURP_L   = "#EDE7F6"
PURP_BOX = "#D1C4E9"
ORANGE   = "#FB8C00"
ORAN_L   = "#FFF3E0"
ORAN_BOX = "#FFE0B2"
GREEN    = "#43A047"
GREEN_L  = "#E8F5E9"
GREEN_BOX= "#C8E6C9"
NAVY     = "#0D47A1"
NAVY_L   = "#E3F2FD"
NAVY_BOX = "#BBDEFB"
GRAY     = "#757575"
LGRAY    = "#BDBDBD"

M = 50
CW = W - 2*M
ROW_H = 80
GAP = 14

def rbox(x, y, w, h, fill, outline, r=10, lw=2):
    draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=fill, outline=outline, width=lw)

def txt(x, y, text, font=font_s, color="#333", anchor="mm"):
    draw.text((x, y), text, fill=color, font=font, anchor=anchor)

def arrow_down(cx, y1, y2, color=LGRAY, lw=2, w=0):
    if w > 0:
        draw.rectangle([cx - w//2, y1, cx + w//2, y2 - 6], fill=color)
    else:
        draw.line([(cx, y1), (cx, y2 - 6)], fill=color, width=lw)
    a = 7
    draw.polygon([(cx, y2), (cx - a, y2 - a), (cx + a, y2 - a)], fill=color)

def wide_arrow(y, color="#E0E0E0", label="", label_color=GRAY):
    bar_h = GAP
    cx = M + CW // 2
    draw.rectangle([cx - 60, y, cx + 60, y + bar_h], fill=color)
    a = 10
    draw.polygon([(cx, y + bar_h + a), (cx - a - 4, y + bar_h - 1), (cx + a + 4, y + bar_h - 1)], fill=color)
    if label:
        txt(cx + 80, y + bar_h // 2, label, font_xs, label_color, "lm")

# ===================== TITLE =====================
txt(W // 2, 22, "Summit Gear Co. — Marketing AI+BI Lab Architecture", font_title, BLUE_D)
txt(W // 2, 44, "Database: MARKETING_AI_BI", font_xs, GRAY)

# ===================== ROW Y positions =====================
y0 = 62
rows = {}
cur = y0
for name in ["marketplace", "source", "dynamic", "ml_ai", "semantic", "agent", "dashboard"]:
    rows[name] = cur
    h = ROW_H if name != "ml_ai" else 120
    cur += h + GAP

# ===================== ROW 1: Marketplace =====================
y = rows["marketplace"]
rbox(M, y, CW, 55, BLUE_L, BLUE, lw=1, r=8)
txt(M + 12, y + 12, "Snowflake Marketplace (optional)", font_h2, BLUE_D, "lm")
items = ["Economic Indicators", "Customer 360", "Consumer Attitudes", "Weather Source"]
mp_w = CW - 40
iw = mp_w // len(items)
for i, name in enumerate(items):
    ix = M + 15 + i * iw
    rbox(ix, y + 26, iw - 8, 22, "#FFFFFF", BLUE, r=6, lw=1)
    txt(ix + (iw - 8) // 2, y + 37, name, font_xs, "#555")

# ===================== ARROW =====================
wide_arrow(y + 55, "#B3E5FC")

# ===================== ROW 2: Source Tables =====================
y = rows["source"]
rbox(M, y, CW, ROW_H, BLUE_L, BLUE)
txt(M + 12, y + 12, "MARKETING_RAW  —  Source Tables", font_h2, BLUE_D, "lm")
tables = [
    ("WHOLESALE\nPARTNERS", "30"),
    ("CAMPAIGNS", "70"),
    ("CUSTOMERS", "8K"),
    ("ORDERS", "29K"),
    ("MARKETING\nSPEND", "4.5K"),
    ("PRODUCT\nREVIEWS", "1.5K"),
]
tw = (CW - 40) // len(tables)
for i, (name, cnt) in enumerate(tables):
    tx = M + 20 + i * tw
    rbox(tx, y + 28, tw - 10, 42, BLUE_BOX, BLUE, r=6, lw=1)
    lines = name.split("\n")
    if len(lines) == 1:
        txt(tx + (tw - 10) // 2, y + 44, lines[0], font_xs, "#333")
    else:
        txt(tx + (tw - 10) // 2, y + 39, lines[0], font_xs, "#333")
        txt(tx + (tw - 10) // 2, y + 50, lines[1], font_xs, "#333")
    txt(tx + (tw - 10) // 2, y + 62, cnt + " rows", font_xs, GRAY)

# ===================== ARROW =====================
wide_arrow(y + ROW_H, "#B3E5FC", "auto-refresh")

# ===================== ROW 3: Dynamic Tables =====================
y = rows["dynamic"]
rbox(M, y, CW, ROW_H, BLUE_L, CYAN)
txt(M + 12, y + 12, "MARKETING_ANALYTICS  —  Dynamic Tables  (TARGET_LAG: 1d / DOWNSTREAM)", font_h2, BLUE_D, "lm")
dts = [
    ("DT_DAILY\nREVENUE", "1.3K"),
    ("DT_CAMPAIGN\nMETRICS", "70"),
    ("DT_PARTNER\nPERF", "30"),
    ("DT_CUSTOMER\nSEGMENTS", "8K"),
    ("DT_FORECAST\nINPUT", "7.3K"),
    ("DT_SPEND\nDAILY", "3.1K"),
    ("DT_SPEND\nFC_INPUT", "3.1K"),
    ("DT_PRODUCT\nREVENUE", "60"),
]
tw = (CW - 30) // len(dts)
for i, (name, cnt) in enumerate(dts):
    tx = M + 15 + i * tw
    rbox(tx, y + 28, tw - 8, 42, BLUE_BOX, CYAN, r=6, lw=1)
    lines = name.split("\n")
    txt(tx + (tw - 8) // 2, y + 39, lines[0], font_xs, "#333")
    txt(tx + (tw - 8) // 2, y + 50, lines[1], font_xs, "#333")
    txt(tx + (tw - 8) // 2, y + 62, cnt + " rows", font_xs, GRAY)

# ===================== ARROW (split into two lanes) =====================
ya = y + ROW_H
bar_h = GAP
left_cx = M + CW // 4
right_cx = M + 3 * CW // 4
draw.rectangle([left_cx - 40, ya, left_cx + 40, ya + bar_h], fill="#D1C4E9")
draw.polygon([(left_cx, ya + bar_h + 8), (left_cx - 10, ya + bar_h - 1), (left_cx + 10, ya + bar_h - 1)], fill="#D1C4E9")
txt(left_cx + 55, ya + bar_h // 2, "ML training", font_xs, GRAY, "lm")

draw.rectangle([right_cx - 40, ya, right_cx + 40, ya + bar_h], fill=ORAN_BOX)
draw.polygon([(right_cx, ya + bar_h + 8), (right_cx - 10, ya + bar_h - 1), (right_cx + 10, ya + bar_h - 1)], fill=ORAN_BOX)
txt(right_cx + 55, ya + bar_h // 2, "AI inference", font_xs, GRAY, "lm")

# ===================== ROW 4: ML (left) + AI (right) =====================
y = rows["ml_ai"]
half = (CW - 10) // 2

rbox(M, y, half, 120, PURP_L, PURPLE)
txt(M + 12, y + 12, "Snowflake ML Models", font_h2, "#4527A0", "lm")

txt(M + 15, y + 30, "FORECAST (3 models)", font_b, PURPLE, "lm")
fc = ["Total Revenue", "By Product/Channel", "Spend by Subchannel"]
for i, m in enumerate(fc):
    bx = M + 15 + i * 175
    rbox(bx, y + 42, 165, 22, PURP_BOX, PURPLE, r=5, lw=1)
    txt(bx + 82, y + 53, m, font_xs, "#333")

rbox(M + 545, y + 36, 90, 32, "#F3E5F5", PURPLE, r=5, lw=1)
txt(M + 590, y + 46, "FORECAST", font_xs, PURPLE)
txt(M + 590, y + 57, "RESULTS  1.7K", font_xs, GRAY)

txt(M + 15, y + 70, "ANOMALY_DETECTION (3 models)", font_b, PURPLE, "lm")
ad = ["Spend by Subchannel", "DTC Conversion", "Wholesale by Product"]
for i, m in enumerate(ad):
    bx = M + 15 + i * 175
    rbox(bx, y + 82, 165, 22, PURP_BOX, PURPLE, r=5, lw=1)
    txt(bx + 82, y + 93, m, font_xs, "#333")

rbox(M + 545, y + 76, 90, 32, "#F3E5F5", PURPLE, r=5, lw=1)
txt(M + 590, y + 86, "ANOMALY", font_xs, PURPLE)
txt(M + 590, y + 97, "RESULTS  3.2K", font_xs, GRAY)

rbox(M + half + 10, y, half, 120, ORAN_L, ORANGE)
txt(M + half + 22, y + 12, "Cortex AI Functions", font_h2, "#E65100", "lm")

ai_items = [
    ("SENTIMENT", "1,500 results"),
    ("CLASSIFY_TEXT", "70 results"),
    ("EXTRACT_ANSWER", "1,500 results"),
    ("COMPLETE", "70 results"),
    ("SUMMARIZE_AGG", "2 results"),
]
ai_col_w = (half - 30) // 3
row1 = ai_items[:3]
row2 = ai_items[3:]
for i, (fn, cnt) in enumerate(row1):
    bx = M + half + 20 + i * ai_col_w
    rbox(bx, y + 30, ai_col_w - 8, 36, ORAN_BOX, ORANGE, r=5, lw=1)
    txt(bx + (ai_col_w - 8) // 2, y + 42, fn, font_xs, "#333")
    txt(bx + (ai_col_w - 8) // 2, y + 55, cnt, font_xs, GRAY)
for i, (fn, cnt) in enumerate(row2):
    bx = M + half + 20 + i * ai_col_w
    rbox(bx, y + 74, ai_col_w - 8, 36, ORAN_BOX, ORANGE, r=5, lw=1)
    txt(bx + (ai_col_w - 8) // 2, y + 86, fn, font_xs, "#333")
    txt(bx + (ai_col_w - 8) // 2, y + 99, cnt, font_xs, GRAY)

# ===================== ARROW =====================
wide_arrow(y + 120, "#C8E6C9", "feeds semantic layer")

# ===================== ROW 5: Semantic Views =====================
y = rows["semantic"]
rbox(M, y, CW, ROW_H, GREEN_L, GREEN)
txt(M + 12, y + 12, "Semantic Views  —  Cortex Analyst (Text-to-SQL)", font_h2, "#2E7D32", "lm")

svs = [
    ("SV_SUMMIT_GEAR_MARKETING", "10 entities: revenue, campaigns,\ncustomers, spend, partners"),
    ("SV_SUMMIT_GEAR_ML", "3 entities: forecasts,\nanomalies, importance"),
    ("SV_SUMMIT_GEAR_REVIEWS", "3 entities: sentiment,\nextracts, themes"),
]
sw = (CW - 40) // 3
for i, (name, desc) in enumerate(svs):
    sx = M + 20 + i * sw
    rbox(sx, y + 28, sw - 12, 42, GREEN_BOX, GREEN, r=6, lw=1)
    txt(sx + (sw - 12) // 2, y + 42, name, font_s, "#333")
    txt(sx + (sw - 12) // 2, y + 58, desc.split("\n")[0], font_xs, GRAY)

# ===================== ARROW =====================
wide_arrow(y + ROW_H, "#BBDEFB", "tool binding")

# ===================== ROW 6: Cortex Agent =====================
y = rows["agent"]
rbox(M, y, CW, ROW_H, NAVY_L, NAVY)
txt(M + 12, y + 12, "Cortex Agent  —  SUMMIT_GEAR_AGENT  (REST API, 3 tools)", font_h2, NAVY, "lm")

tools = [
    ("MarketingAnalyst", "SV_SUMMIT_GEAR_MARKETING"),
    ("MLAnalyst", "SV_SUMMIT_GEAR_ML"),
    ("ReviewAnalyst", "SV_SUMMIT_GEAR_REVIEWS"),
]
tw2 = (CW - 40) // 3
for i, (tname, sv) in enumerate(tools):
    tx = M + 20 + i * tw2
    rbox(tx, y + 28, tw2 - 12, 42, NAVY_BOX, NAVY, r=8, lw=1)
    txt(tx + (tw2 - 12) // 2, y + 42, tname, font_b, NAVY)
    txt(tx + (tw2 - 12) // 2, y + 57, "text_to_sql  \u2192  " + sv, font_xs, GRAY)

# ===================== ARROW =====================
wide_arrow(y + ROW_H, "#BBDEFB", "powers dashboard")

# ===================== ROW 7: Dashboard =====================
y = rows["dashboard"]
rbox(M, y, CW, ROW_H, NAVY_L, NAVY, lw=2)
txt(M + 12, y + 12, "Streamlit-in-Snowflake Dashboard  (Altair SVG charts  |  SEMANTIC_VIEW() queries  |  Agent REST API)", font_h2, NAVY, "lm")

pages = [
    "KPI Overview",
    "Channel Deep Dive",
    "Forecasting & Anomalies",
    "AI Insights",
    "Marketing Agent",
]
pw = (CW - 40) // len(pages)
for i, pname in enumerate(pages):
    px2 = M + 20 + i * pw
    rbox(px2, y + 30, pw - 10, 35, NAVY_BOX, NAVY, r=6, lw=1)
    txt(px2 + (pw - 10) // 2, y + 47, pname, font_s, "#333")

# ===================== LEGEND =====================
lx = W - 235
ly = 830
rbox(lx, ly, 230, 125, "#FAFAFA", "#DDD", lw=1, r=6)
txt(lx + 12, ly + 12, "Legend", font_b, "#333", "lm")
legend = [
    (BLUE_BOX, "Source / Dynamic Tables"),
    (PURP_BOX, "ML Models & Results"),
    (ORAN_BOX, "Cortex AI Functions"),
    (GREEN_BOX, "Semantic Views"),
    (NAVY_BOX, "Agent & Dashboard"),
]
for i, (c, label) in enumerate(legend):
    ry = ly + 28 + i * 18
    draw.rounded_rectangle([lx + 12, ry, lx + 28, ry + 12], radius=2, fill=c, outline="#999", width=1)
    txt(lx + 36, ry + 6, label, font_xs, "#555", "lm")

out = "/Users/gregsuk/Desktop/workspace/CoCo/Demos/marketing_data_ai_bi/architecture_diagram.png"
img.save(out, "PNG", dpi=(150, 150))
print(f"Saved: {out}  ({W}x{H})")
