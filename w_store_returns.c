/* 
 * Legal Notice 
 * 
 * This document and associated source code (the "Work") is a part of a 
 * benchmark specification maintained by the TPC. 
 * 
 * The TPC reserves all right, title, and interest to the Work as provided 
 * under U.S. and international laws, including without limitation all patent 
 * and trademark rights therein. 
 * 
 * No Warranty 
 * 
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
 *     WITH REGARD TO THE WORK. 
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
 * 
 * Contributors:
 * Gradient Systems
 */ 
#include "config.h"
#include "porting.h"
#include <stdio.h>
#include "w_store_returns.h"
#include "w_store_sales.h"
#include "tables.h"
#include "print.h"
#include "pricing.h"
#include "columns.h"
#include "genrand.h"
#include "build_support.h"
#include "nulls.h"
#include "tdefs.h"
#include "stable_rng.h"
#include "params.h"

struct W_STORE_RETURNS_TBL g_w_store_returns;
extern struct W_STORE_SALES_TBL g_w_store_sales;

/*
* Routine: mk_store_returns()
* Purpose: populate a return fact *sync'd with a sales fact*
* Algorithm: 
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
int
mk_w_store_returns (void * row, ds_key_t index)
{
	int res = 0,
		nTemp;
	struct W_STORE_RETURNS_TBL *r;
	struct W_STORE_SALES_TBL *sale = &g_w_store_sales;
	static int bInit = 0;
   tdef *pT = getSimpleTdefsByNumber(STORE_RETURNS);
	
	static decimal_t dMin,
		dMax;
	/* begin locals declarations */
	
	if (row == NULL)
		r = &g_w_store_returns;
	else
		r = row;

	if (!bInit)
	{
        strtodec (&dMin, "1.00");
        strtodec (&dMax, "100000.00");
	}
	
	/* Don't use random NULL generation - we handle NULLs deterministically */
	/* nullSet(&pT->kNullBitMap, SR_NULLS); */
	/*
	* Some of the information in the return is taken from the original sale
	* which has been regenerated
	*/
	r->sr_ticket_number = sale->ss_ticket_number;
	r->sr_item_sk = sale->ss_sold_item_sk;
	memcpy((void *)&r->sr_pricing, (void *)&sale->ss_pricing, sizeof(ds_pricing_t));

	/*
	 * some of the fields are conditionally taken from the sale 
	 */
	int nScale = get_int("SCALE");
	/* Use combination of ticket_number and item_sk for uniqueness */
	ds_key_t item_part = (sale->ss_sold_item_sk == -1) ? 99999L : (sale->ss_sold_item_sk % 100000L);
	ds_key_t combined_key = sale->ss_ticket_number * 100000L + item_part;
	
	/* If sale customer is NULL, return must also be NULL */
	if (sale->ss_sold_customer_sk == -1) {
		r->sr_customer_sk = -1;  /* Preserve NULL */
	} else {
		/* Sale has customer, so return can have customer or different customer */
		int rand_val = stable_genrand_integer(STORE_RETURNS, nScale, combined_key, 1, 100, SR_TICKET_NUMBER);
		if (rand_val < SR_SAME_CUSTOMER) {
			r->sr_customer_sk = sale->ss_sold_customer_sk;  /* Same customer */
		} else {
			/* Different customer - when sale has customer, return must have customer too */
			r->sr_customer_sk = stable_mk_join(SR_CUSTOMER_SK, CUSTOMER, nScale, combined_key, SR_CUSTOMER_SK);
		}
	}

	/*
	* the rest of the columns are generated for this specific return
	*/
	/* Use stable functions for ALL fields to ensure consistency */
	r->sr_returned_date_sk = stable_mk_join(SR_RETURNED_DATE_SK, DATE, nScale, combined_key + sale->ss_sold_date_sk, SR_RETURNED_DATE_SK);
	r->sr_returned_time_sk = stable_genrand_integer(STORE_RETURNS, nScale, combined_key, (8 * 3600) - 1, (17 * 3600) - 1, SR_RETURNED_TIME_SK);
	r->sr_cdemo_sk =
		stable_mk_join(SR_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, nScale, combined_key, SR_CDEMO_SK);
	r->sr_hdemo_sk =
		stable_mk_join(SR_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, nScale, combined_key, SR_HDEMO_SK);
	r->sr_addr_sk = stable_mk_join(SR_ADDR_SK, CUSTOMER_ADDRESS, nScale, combined_key, SR_ADDR_SK);
	r->sr_store_sk = stable_mk_join(SR_STORE_SK, STORE, nScale, combined_key, SR_STORE_SK);
	r->sr_reason_sk = stable_mk_join(SR_REASON_SK, REASON, nScale, combined_key, SR_REASON_SK);
	r->sr_pricing.quantity = stable_genrand_integer(STORE_RETURNS, nScale, combined_key,
		1, sale->ss_pricing.quantity, SR_PRICING);
	set_pricing(SR_PRICING, &r->sr_pricing);
	
	return (res);
}

/*
* Routine: 
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
int
pr_w_store_returns(void *row)
{
	struct W_STORE_RETURNS_TBL *r;


	if (row == NULL)
		r = &g_w_store_returns;
	else
		r = row;
	print_start(STORE_RETURNS);
	print_key(SR_RETURNED_DATE_SK, r->sr_returned_date_sk, 1);
	print_key(SR_RETURNED_TIME_SK, r->sr_returned_time_sk, 1);
	print_key(SR_ITEM_SK, r->sr_item_sk, 1);
	print_key(SR_CUSTOMER_SK, r->sr_customer_sk, 1);
	print_key(SR_CDEMO_SK, r->sr_cdemo_sk, 1);
	print_key(SR_HDEMO_SK, r->sr_hdemo_sk, 1);
	print_key(SR_ADDR_SK, r->sr_addr_sk, 1);
	print_key(SR_STORE_SK, r->sr_store_sk, 1);
	print_key(SR_REASON_SK, r->sr_reason_sk, 1);
	print_key(SR_TICKET_NUMBER, r->sr_ticket_number, 1);
	print_integer(SR_PRICING_QUANTITY, r->sr_pricing.quantity, 1);
	print_decimal(SR_PRICING_NET_PAID, &r->sr_pricing.net_paid, 1);
	print_decimal(SR_PRICING_EXT_TAX, &r->sr_pricing.ext_tax, 1);
	print_decimal(SR_PRICING_NET_PAID_INC_TAX, &r->sr_pricing.net_paid_inc_tax, 1);
	print_decimal(SR_PRICING_FEE, &r->sr_pricing.fee, 1);
	print_decimal(SR_PRICING_EXT_SHIP_COST, &r->sr_pricing.ext_ship_cost, 1);
	print_decimal(SR_PRICING_REFUNDED_CASH, &r->sr_pricing.refunded_cash, 1);
	print_decimal(SR_PRICING_REVERSED_CHARGE, &r->sr_pricing.reversed_charge, 1);
	print_decimal(SR_PRICING_STORE_CREDIT, &r->sr_pricing.store_credit, 1);
	print_decimal(SR_PRICING_NET_LOSS, &r->sr_pricing.net_loss, 0);
	print_end(STORE_RETURNS);
	
	return(0);
}

/*
* Routine: 
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
int 
ld_w_store_returns(void *pSrc)
{
	struct W_STORE_RETURNS_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_store_returns;
	else
		r = pSrc;
	
	return(0);
}

