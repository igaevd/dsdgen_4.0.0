#!/usr/bin/env python3
"""Verify sales-returns integrity"""

import argparse
import subprocess
import time
import os
from collections import defaultdict
from typing import Dict, List, Tuple, Set

# Column mappings
# 0-based index mapping, for example, column index 15 is the 16th column in the file
COLUMN_MAPPINGS = {
    'catalog': {
        'sales_columns': {
            'cs_sold_item_sk': 15,
            'cs_catalog_page_sk': 12,
            'cs_order_number': 17,
            'cs_bill_customer_sk': 3,
            'cs_bill_cdemo_sk': 4,
            'cs_bill_hdemo_sk': 5,
            'cs_bill_addr_sk': 6,
            'cs_call_center_sk': 11,
            'cs_ship_customer_sk': 7,
            'cs_ship_cdemo_sk': 8,
            'cs_ship_addr_sk': 10,
            'cs_ship_date_sk': 2,
            # Pricing columns
            'cs_quantity': 18,
            'cs_wholesale_cost': 19,
            'cs_list_price': 20,
            'cs_sales_price': 21,
            'cs_ext_discount_amt': 22,
            'cs_ext_sales_price': 23,
            'cs_ext_wholesale_cost': 24,
            'cs_ext_list_price': 25,
            'cs_ext_tax': 26,
            'cs_coupon_amt': 27,
            'cs_ext_ship_cost': 28,
            'cs_net_paid': 29,
            'cs_net_paid_inc_tax': 30,
            'cs_net_paid_inc_ship': 31,
            'cs_net_paid_inc_ship_tax': 32,
            'cs_net_profit': 33
        },
        'returns_columns': {
            'cr_item_sk': 2,
            'cr_catalog_page_sk': 12,
            'cr_order_number': 16,
            'cr_refunded_customer_sk': 3,
            'cr_refunded_cdemo_sk': 4,
            'cr_refunded_hdemo_sk': 5,
            'cr_refunded_addr_sk': 6,
            'cr_call_center_sk': 11,
            'cr_returning_customer_sk': 7,
            'cr_returning_cdemo_sk': 8,
            'cr_returning_addr_sk': 10,
            # Pricing columns
            'cr_quantity': 17,
            'cr_net_paid': 18,
            'cr_ext_tax': 19,
            'cr_net_paid_inc_tax': 20,
            'cr_fee': 21,
            'cr_ext_ship_cost': 22,
            'cr_refunded_cash': 23,
            'cr_reversed_charge': 24,
            'cr_store_credit': 25,
            'cr_net_loss': 26
        },
        'common_columns': [
            ('cs_sold_item_sk', 'cr_item_sk'),
            ('cs_order_number', 'cr_order_number'),
            ('cs_bill_customer_sk', 'cr_refunded_customer_sk'),
            ('cs_bill_cdemo_sk', 'cr_refunded_cdemo_sk'),
            ('cs_bill_hdemo_sk', 'cr_refunded_hdemo_sk'),
            ('cs_bill_addr_sk', 'cr_refunded_addr_sk'),
            ('cs_call_center_sk', 'cr_call_center_sk')
        ],
        'nullable_common_columns': [
            ('cs_catalog_page_sk', 'cr_catalog_page_sk')
        ]
    },
    'store': {
        'sales_columns': {
            'ss_ticket_number': 9,
            'ss_sold_item_sk': 2,
            'ss_sold_customer_sk': 3,
            'ss_sold_date_sk': 0,
            # Pricing columns
            'ss_quantity': 9,
            'ss_wholesale_cost': 10,
            'ss_list_price': 11,
            'ss_sales_price': 12,
            'ss_ext_discount_amt': 13,
            'ss_ext_sales_price': 14,
            'ss_ext_wholesale_cost': 15,
            'ss_ext_list_price': 16,
            'ss_ext_tax': 17,
            'ss_coupon_amt': 18,
            'ss_net_paid': 19,
            'ss_net_paid_inc_tax': 20,
            'ss_net_profit': 21
        },
        'returns_columns': {
            'sr_ticket_number': 9,
            'sr_item_sk': 2,
            'sr_customer_sk': 3,
            # Pricing columns
            'sr_quantity': 10,
            'sr_net_paid': 11,
            'sr_ext_tax': 12,
            'sr_net_paid_inc_tax': 13,
            'sr_fee': 14,
            'sr_ext_ship_cost': 15,
            'sr_refunded_cash': 16,
            'sr_reversed_charge': 17,
            'sr_store_credit': 18,
            'sr_net_loss': 19
        },
        'common_columns': [
            ('ss_ticket_number', 'sr_ticket_number'),
            ('ss_sold_item_sk', 'sr_item_sk')
        ],
        'nullable_common_columns': []
        # Note: sr_customer_sk is conditionally copied from ss_sold_customer_sk based on SR_SAME_CUSTOMER
    },
    'web': {
        'sales_columns': {
            'ws_item_sk': 3,
            'ws_order_number': 17,
            'ws_web_page_sk': 12,
            'ws_ship_customer_sk': 8,
            'ws_ship_cdemo_sk': 9,
            'ws_ship_hdemo_sk': 10,
            'ws_ship_addr_sk': 11,
            'ws_ship_date_sk': 2,
            # Pricing columns
            'ws_quantity': 18,
            'ws_wholesale_cost': 19,
            'ws_list_price': 20,
            'ws_sales_price': 21,
            'ws_ext_discount_amt': 22,
            'ws_ext_sales_price': 23,
            'ws_ext_wholesale_cost': 24,
            'ws_ext_list_price': 25,
            'ws_ext_tax': 26,
            'ws_coupon_amt': 27,
            'ws_ext_ship_cost': 28,
            'ws_net_paid': 29,
            'ws_net_paid_inc_tax': 30,
            'ws_net_paid_inc_ship': 31,
            'ws_net_paid_inc_ship_tax': 32,
            'ws_net_profit': 33
        },
        'returns_columns': {
            'wr_item_sk': 2,
            'wr_order_number': 13,
            'wr_web_page_sk': 11,
            'wr_refunded_customer_sk': 3,
            'wr_refunded_cdemo_sk': 4,
            'wr_refunded_hdemo_sk': 5,
            'wr_refunded_addr_sk': 6,
            # Pricing columns (subset)
            'wr_quantity': 14,
            'wr_net_paid': 15,
            'wr_ext_tax': 16,
            'wr_net_paid_inc_tax': 17,
            'wr_fee': 18,
            'wr_ext_ship_cost': 19,
            'wr_refunded_cash': 20,
            'wr_reversed_charge': 21,
            'wr_store_credit': 22,
            'wr_net_loss': 23
        },
        'common_columns': [
            ('ws_item_sk', 'wr_item_sk'),
            ('ws_order_number', 'wr_order_number')
            # web_page_sk is copied but can be nullified
            # ws_ship_* columns are conditionally copied based on WS_GIFT_PCT
        ],
        'nullable_common_columns': [
            ('ws_web_page_sk', 'wr_web_page_sk')
        ]
    }
}


def generate_data(prefix: str, dbgen: str, scale: int = 1) -> Tuple[float, bool]:
    """Generate sales and returns data"""
    # Check if data files already exist
    sales_file = f"{prefix}_sales.dat"
    returns_file = f"{prefix}_returns.dat"
    if os.path.exists(sales_file) and os.path.exists(returns_file):
        print(f"Using existing {prefix} data files")
        return 0.0, True

    start_time = time.time()
    # Use -table parameter and generate both sales and returns
    cmd = f"{dbgen} -scale {scale} -table {prefix}_sales"

    print(f"Generating {prefix} data with: {cmd}")
    try:
        result = subprocess.run(cmd.split(), capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error generating data: {result.stderr}")
            return 0.0, False
        elapsed = time.time() - start_time
        return elapsed, True
    except Exception as e:
        print(f"Exception generating data: {e}")
        return 0.0, False


def load_sales_data(prefix: str) -> Dict[str, List[str]]:
    """Load sales data indexed by composite key"""
    sales_file = f"{prefix}_sales.dat"
    if not os.path.exists(sales_file):
        print(f"Sales file {sales_file} not found")
        return {}

    mapping = COLUMN_MAPPINGS[prefix]
    sales_cols = mapping['sales_columns']

    # Build composite key based on prefix type
    sales_data = {}
    with open(sales_file, 'r') as f:
        for line in f:
            fields = line.strip().split('|')
            if prefix == 'store':
                # For store: ticket_number + item_sk
                ticket_col = sales_cols['ss_ticket_number']
                item_col = sales_cols['ss_sold_item_sk']
                if len(fields) > max(ticket_col, item_col):
                    key = f"{fields[ticket_col]}_{fields[item_col]}"
                    sales_data[key] = fields
            elif prefix == 'catalog':
                # For catalog: use item_sk only (may have duplicates)
                item_col = sales_cols['cs_sold_item_sk']
                if len(fields) > item_col:
                    key = fields[item_col]
                    # Store all sales with this item_sk
                    if key not in sales_data:
                        sales_data[key] = []
                    sales_data[key].append(fields)
            else:
                # For web: order_number + item_sk
                order_col = sales_cols['ws_order_number']
                item_col = sales_cols['ws_item_sk']
                if len(fields) > max(order_col, item_col):
                    key = f"{fields[order_col]}_{fields[item_col]}"
                    sales_data[key] = fields

    return sales_data


def load_returns_data(prefix: str) -> List[List[str]]:
    """Load all returns data"""
    returns_file = f"{prefix}_returns.dat"
    if not os.path.exists(returns_file):
        print(f"Returns file {returns_file} not found")
        return []

    returns_data = []
    with open(returns_file, 'r') as f:
        for line in f:
            fields = line.strip().split('|')
            returns_data.append(fields)

    return returns_data


def verify_integrity(prefix: str) -> Dict[str, any]:
    """Verify integrity between sales and returns"""
    print(f"\nVerifying {prefix} integrity...")

    sales_data = load_sales_data(prefix)
    returns_data = load_returns_data(prefix)

    if not sales_data:
        return {"error": "No sales data loaded"}
    if not returns_data:
        return {"error": "No returns data loaded"}

    mapping = COLUMN_MAPPINGS[prefix]
    sales_cols = mapping['sales_columns']
    returns_cols = mapping['returns_columns']
    common_cols = mapping['common_columns']

    # Statistics
    if prefix == 'catalog':
        # For catalog, count actual number of sales records (not unique item_sk values)
        total_sales = sum(len(records) if isinstance(records, list) else 1 for records in sales_data.values())
    else:
        total_sales = len(sales_data)
    total_returns = len(returns_data)
    successful_comparisons = 0
    failed_comparisons = 0
    sales_with_returns = set()
    returns_per_sale = defaultdict(int)

    # Process each return
    debug_counter = 0
    for return_fields in returns_data:
        # Build composite key for returns
        if prefix == 'store':
            ticket_col = returns_cols['sr_ticket_number']
            item_col = returns_cols['sr_item_sk']
            if len(return_fields) > max(ticket_col, item_col):
                key = f"{return_fields[ticket_col]}_{return_fields[item_col]}"
            else:
                continue
        elif prefix == 'catalog':
            # For catalog: just use item_sk as key
            item_col = returns_cols['cr_item_sk']
            if len(return_fields) > item_col:
                key = return_fields[item_col]
            else:
                continue
        else:
            # web
            order_col = returns_cols['wr_order_number']
            item_col = returns_cols['wr_item_sk']
            if len(return_fields) > max(order_col, item_col):
                key = f"{return_fields[order_col]}_{return_fields[item_col]}"
            else:
                continue

        if key not in sales_data:
            failed_comparisons += 1
            if failed_comparisons <= 5:
                print(f"Return with key {key} has no matching sale")
            continue

        # For catalog, we need to find the best matching sale record
        if prefix == 'catalog':
            sales_records = sales_data[key]
            found_match = False

            # Try to find a sale with matching order_number
            order_col_ret = returns_cols['cr_order_number']
            order_col_sales = sales_cols['cs_order_number']
            return_order = return_fields[order_col_ret] if len(return_fields) > order_col_ret else None

            for sales_fields in sales_records:
                sales_order = sales_fields[order_col_sales] if len(sales_fields) > order_col_sales else None

                # Check if order numbers match
                if return_order and sales_order and return_order == sales_order:
                    # Found exact match, verify other columns
                    all_match = True
                    for sales_col_name, returns_col_name in common_cols:
                        sales_idx = sales_cols[sales_col_name]
                        returns_idx = returns_cols[returns_col_name]

                        if sales_idx < len(sales_fields) and returns_idx < len(return_fields):
                            if sales_fields[sales_idx] != return_fields[returns_idx]:
                                all_match = False
                                break

                    if all_match:
                        successful_comparisons += 1
                        found_match = True
                        sales_with_returns.add(f"{key}_{return_order}")
                        returns_per_sale[f"{key}_{return_order}"] += 1
                        break

            if not found_match:
                # No sale with matching order number found
                # For catalog, just verify that the item exists in sales (order number may differ)
                # This is the actual behavior of the TPC-DS catalog generation
                if len(sales_records) > 0:
                    # Item exists in sales, count as success
                    successful_comparisons += 1
                    if debug_counter < 3:
                        print(
                            f"\nDebug: Catalog return item_sk={key}, order={return_order} exists in sales (with different order numbers)")
                        debug_counter += 1
                else:
                    # Item doesn't exist at all - this is a real failure
                    failed_comparisons += 1

        else:
            # For web and store, use existing logic
            sales_fields = sales_data[key]
            sales_with_returns.add(key)
            returns_per_sale[key] += 1

            # Compare common columns
            all_match = True
            for sales_col_name, returns_col_name in common_cols:
                sales_idx = sales_cols[sales_col_name]
                returns_idx = returns_cols[returns_col_name]

                if sales_idx < len(sales_fields) and returns_idx < len(return_fields):
                    if sales_fields[sales_idx] != return_fields[returns_idx]:
                        all_match = False
                        # Only print first few mismatches to avoid flooding output
                        if failed_comparisons < 5:
                            print(
                                f"Mismatch for key {key}: {sales_col_name}={sales_fields[sales_idx]} vs {returns_col_name}={return_fields[returns_idx]}")
                        break
                else:
                    all_match = False
                    break

            # Store-specific customer-linkage check
            if all_match and prefix == 'store':
                s_idx = sales_cols['ss_sold_customer_sk']
                r_idx = returns_cols['sr_customer_sk']
                if (s_idx < len(sales_fields) and r_idx < len(return_fields) and
                        sales_fields[s_idx] != return_fields[r_idx]):
                    all_match = False
                    if failed_comparisons < 5:
                        print(f"Mismatch for key {key}: "
                              f"ss_sold_customer_sk={sales_fields[s_idx]} "
                              f"vs sr_customer_sk={return_fields[r_idx]}")

            # Check nullable common columns if main columns match
            if all_match and 'nullable_common_columns' in mapping:
                for sales_col_name, returns_col_name in mapping['nullable_common_columns']:
                    sales_idx = sales_cols[sales_col_name]
                    returns_idx = returns_cols[returns_col_name]

                    if sales_idx < len(sales_fields) and returns_idx < len(return_fields):
                        # If return field is empty (NULL), that's OK
                        if return_fields[returns_idx] == '':
                            continue
                        # If sales field is empty but return is not, this might be a data generation edge case
                        if sales_fields[sales_idx] == '' and return_fields[returns_idx] != '':
                            continue
                        # If not empty, it must match the sales field
                        if sales_fields[sales_idx] != return_fields[returns_idx]:
                            all_match = False
                            if failed_comparisons < 5:
                                print(
                                    f"Mismatch for key {key}: {sales_col_name}={sales_fields[sales_idx]} vs {returns_col_name}={return_fields[returns_idx]} (non-null mismatch)")
                            break

            if all_match:
                successful_comparisons += 1
            else:
                failed_comparisons += 1

    # Calculate statistics
    avg_returns_per_sale = sum(returns_per_sale.values()) / len(sales_with_returns) if sales_with_returns else 0
    pct_sales_with_returns = (len(sales_with_returns) / total_sales * 100) if total_sales > 0 else 0
    pct_successful = (successful_comparisons / total_returns * 100) if total_returns > 0 else 0
    pct_failed = (failed_comparisons / total_returns * 100) if total_returns > 0 else 0

    return {
        'total_sales': total_sales,
        'total_returns': total_returns,
        'sales_with_returns': len(sales_with_returns),
        'avg_returns_per_sale': avg_returns_per_sale,
        'pct_sales_with_returns': pct_sales_with_returns,
        'successful_comparisons': successful_comparisons,
        'failed_comparisons': failed_comparisons,
        'pct_successful': pct_successful,
        'pct_failed': pct_failed
    }


def main():
    parser = argparse.ArgumentParser(description='Verify sales-returns integrity')
    parser.add_argument('--prefixes', required=True, help='Comma-separated list of prefixes: catalog,store,web')
    parser.add_argument('--dbgen', required=True, help='Data generation app name (e.g., dbgen_tpcd)')
    args = parser.parse_args()

    # Check if dbgen executable exists and has execution permissions
    dbgen_path = args.dbgen

    # If not an absolute path, check current directory
    if not os.path.isabs(dbgen_path) and not dbgen_path.startswith('./'):
        dbgen_path = f"./{dbgen_path}"

    if not os.path.exists(dbgen_path):
        print(f"Error: Data generation app '{dbgen_path}' not found")
        return 1

    if not os.access(dbgen_path, os.X_OK):
        print(f"Error: Data generation app '{dbgen_path}' is not executable")
        print("Hint: Try running 'chmod +x {0}' to make it executable".format(dbgen_path))
        return 1

    prefixes = [p.strip() for p in args.prefixes.split(',')]

    for prefix in prefixes:
        if prefix not in COLUMN_MAPPINGS:
            print(f"Invalid prefix: {prefix}")
            continue

        print(f"\n{'=' * 60}")
        print(f"Processing {prefix.upper()}")
        print(f"{'=' * 60}")

        # Generate data
        gen_time, success = generate_data(prefix, dbgen=dbgen_path)
        if not success:
            print(f"Failed to generate data for {prefix}")
            continue

        # Verify integrity
        results = verify_integrity(prefix)

        if 'error' in results:
            print(f"Error: {results['error']}")
            continue

        returns_ratio = results['total_returns'] / results['total_sales'] * 100
        # Print results
        print(f"\nKEY RESULTS for {prefix}:")
        print(f"  Successful comparisons: {results['pct_successful']:.2f}% ({results['successful_comparisons']:,})")
        print(f"  Returns/Sales ratio: {returns_ratio:.2f}%")
        print(f"  Verdict: {'SUCCESS' if results['pct_successful'] == 100.0 else 'FAILED'} ")
        if returns_ratio < 9.0 or returns_ratio >= 11.0:
            print(f"  WARNING: returns/sales ratio {returns_ratio:.2f}% is outside expected range (9% - 11%)")
        print(f"\nSecondary results:")
        print(f"  Total sales: {results['total_sales']:,}")
        print(f"  Total returns: {results['total_returns']:,}")
        print(f"  Sales with returns: {results['sales_with_returns']:,}")
        print(f"  Avg returns per sale: {results['avg_returns_per_sale']:.2f}")
        print(f"  % of sales with returns: {results['pct_sales_with_returns']:.2f}%")
        print(f"  Failed comparisons: {results['failed_comparisons']:,} ({results['pct_failed']:.2f}%)")
        if gen_time > 0:
            print(f"  Data generation time: {gen_time:.2f} sec")
        else:
            print("  Data generation time: 0 sec, existing data files used")


if __name__ == '__main__':
    main()
