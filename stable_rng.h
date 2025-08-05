#ifndef STABLE_RNG_H
#define STABLE_RNG_H

#include "r_params.h"

int stable_rand_10pct(int table, int scale, ds_key_t index);
int get_current_table_id(void);
int stable_rand_uniform(int table, int scale, ds_key_t index, int min, int max, int seed);
int stable_rand_percentage(int table, int scale, ds_key_t index, int percentage, int seed);
ds_key_t stable_mk_join(int column, int table, int scale, ds_key_t index, int seed);
int stable_genrand_integer(int table, int scale, ds_key_t index, int min, int max, int seed);
int stable_is_null(int table, int scale, ds_key_t index, int column, int null_pct);

#endif /* STABLE_RNG_H */