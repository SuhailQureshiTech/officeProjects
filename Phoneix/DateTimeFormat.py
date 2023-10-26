from datetime import date

today = date.today()

# dd/mm/YY
d1 = today.strftime("%d/%m/%Y")
print("d1 =", d1)

# Textual month, day and year
d2 = today.strftime("%B %d, %Y")
print("d2 =", d2)

# mm/dd/y
d3 = today.strftime("%m/%d/%Y")
print("d3 =", d3)

# Month abbreviation, day and year
d4 = today.strftime("%b-%d-%Y")
print("d4 =", d4)

# Month abbreviation, day and year   11-Sep-21
d5 = today.strftime("%d-%b-%Y")
print("d5 =", d5)

d6 = today.strftime("%Y-%m-%d")
print("d6 =", d6)
