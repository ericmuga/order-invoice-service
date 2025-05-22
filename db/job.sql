insert into [orders].[dbo].[invoice_data] (
    [ExtDocNo],
    [LineNo],
    [CustNo],
    [Date],
    [SPCode],
    [ShiptoCode],
    [ItemNo],
    [Qty],
    [Location],
    [SUOM],
    [UnitPrice],
    [TotalHeaderAmount],
    [LineAmount],
    [TotalHeaderQty],
    [Type],
    [CUInvoiceNo],
    [CUNo],
    [SigningTime],
    [Published]
  )
SELECT upper([invoice_no]) [ExtDocNo],
  b.[id],
  g.[shop_customer_no] --put a case to toggle customers
,
  cast(a.[created_at] as date),
  [shop_code] --put a case to toggle salespersons
,
  [item_code],
  [qty],
  g.location_code [Location] --put case to toggle locations
,
  upper(f.unit_measure) [SUOM] --OR PC
,
  [price],
  (
    select sum(total)
    from [orders].[dbo].[shop_order_items] as c
    where c.order_id = b.order_id
  ) [TotalHeaderAmount],
  [total] [LineAmount],
  (
    select sum(qty)
    from [orders].[dbo].[shop_order_items] as d
    where d.order_id = b.order_id
  ) [TotalHeaderQty],
  0 [Type],
  [fiscal_mtn] [CUInvoiceNo],
  [fiscal_msn] [CUNo],
  [fiscal_DateTime] [SigningTime],
  0 [Published]
FROM [orders].[dbo].[shop_invoices] as a
  inner join [orders].[dbo].[shop_order_items] as b on a.order_no = b.order_id
  inner join [orders].[dbo].[shop_products] as f on b.item_code = f.code
  inner join [orders].[dbo].[users] as g on left(g.sales_code, 3) = left(a.shop_code, 3)
where not exists(
    select 1
    from [orders].[dbo].[invoice_data]
    where [LineNo] = b.id
      and ExtDocNo = upper([invoice_no])
  )
  and a.created_at >= DATEADD(d, -2, DATEDIFF(d, 0, GETDATE()))
insert into [orders].[dbo].[invoice_data] (
    [ExtDocNo],
    [LineNo],
    [CustNo],
    [Date],
    [SPCode],
    [ShiptoCode],
    [ItemNo],
    [Qty],
    [Location],
    [SUOM],
    [UnitPrice],
    [TotalHeaderAmount],
    [LineAmount],
    [TotalHeaderQty],
    [Type],
    [CUInvoiceNo],
    [CUNo],
    [SigningTime],
    [Published]
  )
SELECT upper([invoice_no]) [ExtDocNo],
  b.[id],
  g.[shop_customer_no] --put a case to toggle customers
,
  cast(cast(a.[created_at] as date) as datetime) [Date],
  [shop_code] --put a case to toggle salespersons
,
  g.[shop_customer_no],
  [item_code],
  [qty],
  g.location_code [Location] --put case to toggle locations
,
  upper(f.unit_measure) [SUOM] --OR PC
,
  [price],
  (
    select sum(total)
    from [orders].[dbo].[shop_order_items] as c
    where c.order_id = b.order_id
  ) [TotalHeaderAmount],
  [total] [LineAmount],
  (
    select sum(qty)
    from [orders].[dbo].[shop_order_items] as d
    where d.order_id = b.order_id
  ) [TotalHeaderQty],
  0 [Type],
  [fiscal_mtn] [CUInvoiceNo],
  [fiscal_msn] [CUNo],
  cast([fiscal_DateTime] as nvarchar) [SigningTime],
  0 [Published]
FROM [orders].[dbo].[shop_invoices] as a
  inner join [orders].[dbo].[shop_order_items] as b on a.order_no = b.order_id
  inner join [orders].[dbo].[shop_products] as f on b.item_code = f.code
  inner join [orders].[dbo].[users] as g on left(g.sales_code, 3) = left(a.shop_code, 3)
where not exists(
    select 1
    from [orders].[dbo].[invoice_data]
    where [LineNo] = b.id
      and ExtDocNo = upper([invoice_no])
  )
  and a.created_at >= DATEADD(d, -2, DATEDIFF(d, 0, GETDATE()))