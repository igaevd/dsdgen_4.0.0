#include "config.h"
#include "porting.h"
#include <string.h>
#include <strings.h>
#include "stable_rng.h"
#include "params.h"
#include "tables.h"
#include "tdefs.h"

/*
 * Get the current table being generated based on the TABLE parameter
 */
int get_current_table_id(void)
{
    char *tname = get_str("TABLE");
    
    if (!strcmp(tname, "web_sales"))
        return WEB_SALES;
    else if (!strcmp(tname, "web_returns"))
        return WEB_RETURNS;
    else if (!strcmp(tname, "catalog_sales"))
        return CATALOG_SALES;
    else if (!strcmp(tname, "catalog_returns"))
        return CATALOG_RETURNS;
    else if (!strcmp(tname, "store_sales"))
        return STORE_SALES;
    else if (!strcmp(tname, "store_returns"))
        return STORE_RETURNS;
    
    return -1; // Unknown table
}

/*
 * Stable random function to determine if a sale should have a return
 * This will always return the same result for the same table/scale/index combination
 */
int stable_rand_10pct(int table, int scale, ds_key_t index)
{
    // Use a simple hash function to create a stable pseudo-random value
    // The hash combines table ID, scale, and index to ensure consistency
    unsigned long hash = table * 1000000L + scale * 10000L + (index % 10000L);
    
    // Mix the bits to get better distribution
    hash = hash * 1103515245L + 12345L;
    hash = (hash / 65536L) % 32768L;
    
    // Return 1 if value falls in the 10% range (0-99 -> 10% chance)
    return (hash % 100) < 10;
}