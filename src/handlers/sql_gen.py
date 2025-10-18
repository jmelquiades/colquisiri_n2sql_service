def partners_search(params):
    where, args = [], []
    q = params.get("q")
    if q:
        # usa un solo placeholder para ambas condiciones
        where.append("(display_name ILIKE '%%' || $%d || '%%' OR email ILIKE '%%' || $%d || '%%')" % (len(args)+1, len(args)+1))
        args.append(q)
    if params.get("company_id") is not None:
        where.append("company_id = $%d" % (len(args)+1)); args.append(int(params["company_id"]))
    where_sql = " WHERE " + " AND ".join(where) if where else ""
    sql = f"""
      SELECT id, display_name, vat, email, company_id
      FROM odoo_replica.stg_res_partner
      {where_sql}
      ORDER BY display_name ASC
      LIMIT 200
    """
    return sql, args

def moves_expiring(params):
    where, args = [], []
    if params.get("start") and params.get("end"):
        where.append("invoice_date_due BETWEEN $%d AND $%d" % (len(args)+1, len(args)+2))
        args.extend([params["start"], params["end"]])
    if params.get("state"):
        where.append("state = $%d" % (len(args)+1)); args.append(params["state"])
    if params.get("partner_id"):
        where.append("partner_id = $%d" % (len(args)+1)); args.append(int(params["partner_id"]))
    where_sql = " WHERE " + " AND ".join(where) if where else ""
    sql = f"""
      SELECT id, name, move_type, state, payment_state, partner_id,
             invoice_date, invoice_date_due, amount_total, amount_residual,
             currency_id, company_id
      FROM odoo_replica.stg_account_move
      {where_sql}
      ORDER BY invoice_date DESC
      LIMIT 200
    """
    return sql, args
