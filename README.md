# sdv_sampling_scale

Environment Details
SDV version: 1.2.0
Python version: 3.10.11
Operating System: Ubuntu 22.04
Problem Description / Error Description
I am looking to generate synthetic data at scale, for two tables (Orders, and transactions) having a relationship between them, where orders is a parent and transactions are a child. After Validating the MultiTableMetadata and applying constraints, I was also able to fit the HMASynthesizer on real data.

Now I am trying to generate the sample in a loop as follows:

```
for i in range(10):
    synthetic_data = synthesizer.sample(scale=10)
    ## Rest of the code
```

While I was able to generate a sample up until the 6th iteration, I am facing a "pandas. error.IntCastingNanError" from the 7th iteration. I have also ensured that there are no missing values in my original/real data. What's strange is that I am able to generate data for some iterations.

I have also tried generating samples out of a loop where for scale=50 I was able to generate data, whereas for scale=100 I am facing the same issue which I am facing when I run in a loop.

Output:
```
 /home/ubuntu/sdv/bin/python /home/ubuntu/datastes/sdv_local.py
Preprocess Tables: 100%|████████████████████████████████████████████████████████████████████████| 2/2 [00:01<00:00,  1.24it/s]

Learning relationships:
(1/1) Tables 'orders' and 'transactions' ('order_id'):  67%|█████████████████████▍          | 332/496 [00:42<00:27,  5.95it/s]/home/ubuntu/sdv/lib/python3.10/site-packages/copulas/multivariate/gaussian.py:119: UserWarning: Unable to fit to a <class 'copulas.univariate.beta.BetaUnivariate'> distribution for column profit. Using a Gaussian distribution instead.
  warnings.warn(warning_message)
(1/1) Tables 'orders' and 'transactions' ('order_id'): 100%|████████████████████████████████| 496/496 [01:04<00:00,  7.71it/s]

Modeling Tables: 100%|██████████████████████████████████████████████████████████████████████████| 1/1 [00:04<00:00,  4.39s/it]
Traceback (most recent call last):
  File "/home/ubuntu/datastes/sdv_globalmart_local.py", line 308, in <module>
    synthetic_data = synthesizer.sample(scale=10)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/multi_table/base.py", line 399, in sample
    sampled_data = self._sample(scale=scale)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/multi_table/hma.py", line 570, in _sample
    self._sample_table(table, scale=scale, sampled_data=sampled_data)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/multi_table/hma.py", line 538, in _sample_table
    self._sample_children(table_name, sampled_data, table_rows)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/multi_table/hma.py", line 405, in _sample_children
    self._sample_child_rows(child_name, table_name, row, sampled_data)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/multi_table/hma.py", line 375, in _sample_child_rows
    table_rows = self._sample_rows(synthesizer, table_name)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/multi_table/hma.py", line 348, in _sample_rows
    return self._process_samples(table_name, sampled_rows)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/multi_table/hma.py", line 319, in _process_samples
    sampled = data_processor.reverse_transform(sampled_rows)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/data_processing/data_processor.py", line 714, in reverse_transform
    reversed_data = constraint.reverse_transform(reversed_data)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/constraints/base.py", line 288, in reverse_transform
    return self._reverse_transform(table_data)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/sdv/constraints/tabular.py", line 719, in _reverse_transform
    table_data[self._column_name] = pd.Series(original_column).astype(self._dtype)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/pandas/core/generic.py", line 6324, in astype
    new_data = self._mgr.astype(dtype=dtype, copy=copy, errors=errors)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/pandas/core/internals/managers.py", line 451, in astype
    return self.apply(
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/pandas/core/internals/managers.py", line 352, in apply
    applied = getattr(b, f)(**kwargs)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/pandas/core/internals/blocks.py", line 511, in astype
    new_values = astype_array_safe(values, dtype, copy=copy, errors=errors)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/pandas/core/dtypes/astype.py", line 242, in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/pandas/core/dtypes/astype.py", line 187, in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/pandas/core/dtypes/astype.py", line 105, in _astype_nansafe
    return _astype_float_to_int_nansafe(arr, dtype, copy)
  File "/home/ubuntu/sdv/lib/python3.10/site-packages/pandas/core/dtypes/astype.py", line 150, in _astype_float_to_int_nansafe
    raise IntCastingNaNError(
pandas.errors.IntCastingNaNError: Cannot convert non-finite values (NA or inf) to integer
```
