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
#include "decimal.h"
#include "w_store_sales.h"
#include "w_store_returns.h"
#include "genrand.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "constants.h"
#include "print.h"
#include "nulls.h"
#include "tdefs.h"
#include "scaling.h"
#include "permute.h"
#include "scd.h"
#include "parallel.h"
#include "stable_rng.h"
#include "params.h"
#ifdef JMS
extern rng_t Streams[];
#endif

struct W_STORE_SALES_TBL g_w_store_sales;
ds_key_t skipDays(int nTable, ds_key_t *pRemainder);
static int *pItemPermutation,
   nItemCount,
	nItemIndex;
static ds_key_t jDate,
   kNewDateIndex;

/*
* mk_store_sales
*/
static void
mk_master (void *row, ds_key_t index)
{
	struct W_STORE_SALES_TBL *r;
	static decimal_t dMin,
		dMax;
	static int bInit = 0,
		nMaxItemCount;
	static ds_key_t kNewDateIndex = 0;

	if (row == NULL)
		r = &g_w_store_sales;
	else
		r = row;

	if (!bInit)
	{
      strtodec (&dMin, "1.00");
      strtodec (&dMax, "100000.00");
		nMaxItemCount = 20;
		jDate = skipDays(STORE_SALES, &kNewDateIndex);
		pItemPermutation = makePermutation(NULL, nItemCount = (int)getIDCount(ITEM), SS_PERMUTATION);
		
		bInit = 1;
	}

	
   while (index > kNewDateIndex)	/* need to move to a new date */
   {
      jDate += 1;
      kNewDateIndex += dateScaling(STORE_SALES, jDate);
   }
		int nScale = get_int("SCALE");
		r->ss_sold_store_sk = stable_mk_join(SS_SOLD_STORE_SK, STORE, nScale, index, SS_SOLD_STORE_SK);
		r->ss_sold_time_sk = stable_mk_join(SS_SOLD_TIME_SK, TIME, nScale, index, SS_SOLD_TIME_SK);
		r->ss_sold_date_sk = stable_mk_join(SS_SOLD_DATE_SK, DATE, nScale, index, SS_SOLD_DATE_SK);
		
		// Check if customer should be NULL (assuming 5% NULL rate)
		if (stable_is_null(STORE_SALES, nScale, index, SS_SOLD_CUSTOMER_SK, 5)) {
			r->ss_sold_customer_sk = -1;  // NULL
		} else {
			r->ss_sold_customer_sk = stable_mk_join(SS_SOLD_CUSTOMER_SK, CUSTOMER, nScale, index, SS_SOLD_CUSTOMER_SK);
		}
		
		r->ss_sold_cdemo_sk = stable_mk_join(SS_SOLD_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, nScale, index, SS_SOLD_CDEMO_SK);
		r->ss_sold_hdemo_sk = stable_mk_join(SS_SOLD_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, nScale, index, SS_SOLD_HDEMO_SK);
		r->ss_sold_addr_sk = stable_mk_join(SS_SOLD_ADDR_SK, CUSTOMER_ADDRESS, nScale, index, SS_SOLD_ADDR_SK);
		r->ss_ticket_number = index;
		nItemIndex = stable_genrand_integer(STORE_SALES, nScale, index, 1, nItemCount, SS_SOLD_ITEM_SK);

      return;
}


static void
mk_detail (void *row, int bPrint)
{
struct W_STORE_RETURNS_TBL ReturnRow;
struct W_STORE_SALES_TBL *r;
tdef *pT = getSimpleTdefsByNumber(STORE_SALES);

	if (row == NULL)
		r = &g_w_store_sales;
	else
		r = row;

   // Don't use nullSet - we handle NULLs deterministically
	// nullSet(&pT->kNullBitMap, SS_NULLS);
	/* 
	 * items need to be unique within an order
	 * use a sequence within the permutation 
	 */
	if (++nItemIndex > nItemCount)
      nItemIndex = 1;
   r->ss_sold_item_sk = matchSCDSK(getPermutationEntry(pItemPermutation, nItemIndex), r->ss_sold_date_sk, ITEM);
	int nScale = get_int("SCALE");
	r->ss_sold_promo_sk = stable_mk_join(SS_SOLD_PROMO_SK, PROMOTION, nScale, r->ss_ticket_number * 1000L + nItemIndex, SS_SOLD_PROMO_SK);
	set_pricing(SS_PRICING, &r->ss_pricing);

	/** 
	* having gone to the trouble to make the sale, now let's see if it gets returned
	* Use stable RNG to ensure consistent returns across separate runs
	*/
	nScale = get_int("SCALE");
	int current_table = get_current_table_id();
	/* Always use STORE_SALES for stable RNG to ensure consistency */
	/* Use combination of ticket_number and item_sk for uniqueness */
	/* Handle NULL item_sk (-1) properly */
	ds_key_t item_part = (r->ss_sold_item_sk == -1) ? 99999L : (r->ss_sold_item_sk % 100000L);
	ds_key_t combined_key = r->ss_ticket_number * 100000L + item_part;
	int should_have_return = stable_rand_10pct(STORE_SALES, nScale, combined_key);
	
	if (should_have_return)
	{
		mk_w_store_returns(&ReturnRow, 1);
		/* Only print returns if we're generating store_returns table */
		if (bPrint && current_table == STORE_RETURNS)
			pr_w_store_returns(&ReturnRow);
	}

	/**
	* now we print out the order and lineitem together as a single row
	* Only print sales if we're generating store_sales table
	*/
	if (bPrint && current_table == STORE_SALES)
		pr_w_store_sales(NULL);
	
	return;
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
pr_w_store_sales(void *row)
{
	struct W_STORE_SALES_TBL *r;
	
	if (row == NULL)
		r = &g_w_store_sales;
	else
		r = row;

	print_start(STORE_SALES);
	print_key(SS_SOLD_DATE_SK, r->ss_sold_date_sk, 1);
	print_key(SS_SOLD_TIME_SK, r->ss_sold_time_sk, 1);
	print_key(SS_SOLD_ITEM_SK, r->ss_sold_item_sk, 1);
	print_key(SS_SOLD_CUSTOMER_SK, r->ss_sold_customer_sk, 1);
	print_key(SS_SOLD_CDEMO_SK, r->ss_sold_cdemo_sk, 1);
	print_key(SS_SOLD_HDEMO_SK, r->ss_sold_hdemo_sk, 1);
	print_key(SS_SOLD_ADDR_SK, r->ss_sold_addr_sk, 1);
	print_key(SS_SOLD_STORE_SK, r->ss_sold_store_sk, 1);
	print_key(SS_SOLD_PROMO_SK, r->ss_sold_promo_sk, 1);
	print_key(SS_TICKET_NUMBER, r->ss_ticket_number, 1);
	print_integer(SS_PRICING_QUANTITY, r->ss_pricing.quantity, 1);
	print_decimal(SS_PRICING_WHOLESALE_COST, &r->ss_pricing.wholesale_cost, 1);
	print_decimal(SS_PRICING_LIST_PRICE, &r->ss_pricing.list_price, 1);
	print_decimal(SS_PRICING_SALES_PRICE, &r->ss_pricing.sales_price, 1);
	print_decimal(SS_PRICING_COUPON_AMT, &r->ss_pricing.coupon_amt, 1);
	print_decimal(SS_PRICING_EXT_SALES_PRICE, &r->ss_pricing.ext_sales_price, 1);
	print_decimal(SS_PRICING_EXT_WHOLESALE_COST, &r->ss_pricing.ext_wholesale_cost, 1);
	print_decimal(SS_PRICING_EXT_LIST_PRICE, &r->ss_pricing.ext_list_price, 1);
	print_decimal(SS_PRICING_EXT_TAX, &r->ss_pricing.ext_tax, 1);
	print_decimal(SS_PRICING_COUPON_AMT, &r->ss_pricing.coupon_amt, 1);
	print_decimal(SS_PRICING_NET_PAID, &r->ss_pricing.net_paid, 1);
	print_decimal(SS_PRICING_NET_PAID_INC_TAX, &r->ss_pricing.net_paid_inc_tax, 1);
	print_decimal(SS_PRICING_NET_PROFIT, &r->ss_pricing.net_profit, 0);
	print_end(STORE_SALES);

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
ld_w_store_sales(void *pSrc)
{
	struct W_STORE_SALES_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_store_sales;
	else
		r = pSrc;
	
	return(0);
}

/*
* mk_store_sales
*/
int
mk_w_store_sales (void *row, ds_key_t index)
{
	int nLineitems,
		i;

   /* build the static portion of an order */
	mk_master(row, index);

   /* set the number of lineitems and build them */
	int nScale = get_int("SCALE");
	nLineitems = stable_genrand_integer(STORE_SALES, nScale, index, 8, 16, SS_TICKET_NUMBER);
   for (i = 1; i <= nLineitems; i++)
   {
	   mk_detail(NULL, 1);
   }

   /**
   * and finally return 1 since we have already printed the rows
   */	
   return (1);
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
vld_w_store_sales(int nTable, ds_key_t kRow, int *Permutation)
{
	int nLineitem,
		nMaxLineitem,
		i;

	row_skip(nTable, kRow - 1);
	row_skip(STORE_RETURNS, kRow - 1);
	jDate = skipDays(STORE_SALES, &kNewDateIndex);		
	mk_master(NULL, kRow);
	int nScale = get_int("SCALE");
	nMaxLineitem = stable_genrand_integer(STORE_SALES, nScale, kRow, 8, 16, SS_TICKET_NUMBER);
	nLineitem = stable_genrand_integer(STORE_SALES, nScale, kRow, 1, nMaxLineitem, SS_PRICING_QUANTITY);
	for (i = 1; i < nLineitem; i++)
	{
		mk_detail(NULL, 0);
	}
	mk_detail(NULL, 1);

	return(0);
}

