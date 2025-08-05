#include "config.h"
#include "porting.h"
#include <string.h>
#include <strings.h>
#include "stable_rng.h"
#include "params.h"
#include "tables.h"
#include "tdefs.h"
#include "scaling.h"

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

/*
 * Stable random function to generate a uniform random number in range [min, max]
 * This will always return the same result for the same table/scale/index/seed combination
 */
int stable_rand_uniform(int table, int scale, ds_key_t index, int min, int max, int seed)
{
    // Use a hash function to create a stable pseudo-random value
    unsigned long hash = table * 1000000L + scale * 10000L + (index % 10000L) + seed * 100L;
    
    // Mix the bits for better distribution
    hash = hash * 1103515245L + 12345L;
    hash = (hash / 65536L) % 32768L;
    
    // Return value in range [min, max]
    int range = max - min + 1;
    return min + (hash % range);
}

/*
 * Stable random function to determine if an event should happen based on percentage
 * This will always return the same result for the same table/scale/index/seed combination
 */
int stable_rand_percentage(int table, int scale, ds_key_t index, int percentage, int seed)
{
    // Use a hash function to create a stable pseudo-random value
    unsigned long hash = table * 1000000L + scale * 10000L + (index % 10000L) + seed * 100L;
    
    // Mix the bits for better distribution
    hash = hash * 1103515245L + 12345L;
    hash = (hash / 65536L) % 32768L;
    
    // Return 1 if value falls within the percentage range
    return (hash % 100) < percentage;
}

/*
 * Stable version of mk_join that always returns the same value for same inputs
 * This ensures consistent foreign key generation across separate runs
 */
ds_key_t stable_mk_join(int column, int table_id, int scale, ds_key_t index, int seed)
{
    // Get the actual row count for the table
    ds_key_t nRowCount = getIDCount(table_id);
    
    // Use hash to generate a stable pseudo-random key
    unsigned long hash = table_id * 1000000L + scale * 10000L + (index % 10000L) + seed * 100L + column;
    
    // Mix the bits for better distribution
    hash = hash * 1103515245L + 12345L;
    hash = (hash / 65536L) % 32768L;
    
    // Return a value in the valid range [1, nRowCount]
    return (hash % nRowCount) + 1;
}

/*
 * Stable version of genrand_integer that always returns the same value for same inputs
 */
int stable_genrand_integer(int table, int scale, ds_key_t index, int min, int max, int seed)
{
    // Use the same logic as stable_rand_uniform
    return stable_rand_uniform(table, scale, index, min, max, seed);
}

/*
 * Stable function to determine if a field should be NULL
 */
int stable_is_null(int table, int scale, ds_key_t index, int column, int null_pct)
{
    // Use hash to determine if this field should be NULL
    unsigned long hash = table * 1000000L + scale * 10000L + (index % 10000L) + column * 100L;
    
    // Mix the bits
    hash = hash * 1103515245L + 12345L;
    hash = (hash / 65536L) % 32768L;
    
    // Return 1 if should be NULL based on percentage
    return (hash % 100) < null_pct;
}