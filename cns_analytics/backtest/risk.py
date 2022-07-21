def get_risk(work_diapason, step, sl_diapason, qty_per_level=1):
    number_of_levels = round(work_diapason / step)
    qty = number_of_levels * qty_per_level
    return (work_diapason / 2 + sl_diapason) * qty
