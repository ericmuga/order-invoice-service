USE [orders]
GO
    /****** Object:  Table [dbo].[invoice_data]    Script Date: 5/22/2025 5:42:35 AM ******/
    IF EXISTS (
        SELECT *
        FROM sys.objects
        WHERE object_id = OBJECT_ID(N'[dbo].[invoice_data]')
            AND type in (N'U')
    ) DROP TABLE [dbo].[invoice_data]
GO
    /****** Object:  Table [dbo].[invoice_data]    Script Date: 5/22/2025 5:42:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO CREATE TABLE [dbo].[invoice_data](
        [ExtDocNo] [varchar](50) NOT NULL,
        [LineNo] [int] NOT NULL,
        [CustNo] [varchar](50) NULL,
        [Date] [date] NULL,
        [SPCode] [varchar](50) NULL,
        [ShiptoCode] [varchar](50) NULL,
        [ItemNo] [varchar](50) NULL,
        [Qty] [decimal](18, 4) NULL,
        [Location] [varchar](50) NULL,
        [SUOM] [varchar](20) NULL,
        [UnitPrice] [decimal](18, 4) NULL,
        [TotalHeaderAmount] [decimal](18, 4) NULL,
        [LineAmount] [decimal](18, 4) NULL,
        [TotalHeaderQty] [decimal](18, 4) NULL,
        [Type] [tinyint] NULL,
        [CUInvoiceNo] [varchar](50) NULL,
        [CUNo] [varchar](50) NULL,
        [SigningTime] [varchar](50) NULL,
        [Published] [tinyint] NULL,
        CONSTRAINT [PK_invoice_data] PRIMARY KEY CLUSTERED ([ExtDocNo] ASC, [LineNo] ASC) WITH (
            PAD_INDEX = OFF,
            STATISTICS_NORECOMPUTE = OFF,
            IGNORE_DUP_KEY = OFF,
            ALLOW_ROW_LOCKS = ON,
            ALLOW_PAGE_LOCKS = ON,
            OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
        ) ON [PRIMARY]
    ) ON [PRIMARY]
GO