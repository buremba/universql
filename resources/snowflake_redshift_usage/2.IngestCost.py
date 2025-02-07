import marimo

__generated_with = "0.1.77"
app = marimo.App()


@app.cell
def _():
    import duckdb

    sf_all = duckdb.query("select * from 'snowset/part.*.parquet' using sample 1 percent (bernoulli)").df()

    sf_all
    return duckdb, sf_all


@app.cell
def _(duckdb):
    query = """
    select

    persistentReadBytesS3 > 0 as persistentReadBytesS3,
    persistentReadBytesCache > 0 as persistentReadBytesCache,
    persistentWriteBytesCache > 0 as persistentWriteBytesCache,
    persistentWriteBytesS3 > 0 as persistentWriteBytesS3,
    intDataWriteBytesLocalSSD > 0 as intDataWriteBytesLocalSSD,
    intDataReadBytesLocalSSD > 0 as intDataReadBytesLocalSSD,
    intDataWriteBytesS3 > 0 as intDataWriteBytesS3,
    intDataReadBytesS3 > 0 as intDataReadBytesS3,
    ioRemoteExternalReadBytes > 0 as ioRemoteExternalReadBytes,
    intDataNetReceivedBytes > 0 as intDataNetReceivedBytes,
    intDataNetSentBytes > 0 as intDataNetSentBytes,

    producedRows == 0 as producedRows0,
    producedRows == 1 as producedRows1,

    returnedRows == 0 as returnedRows0,
    returnedRows == 1 as returnedRows1,

    remoteSeqScanFileOps > 0 as remoteSeqScanFileOps,
    localSeqScanFileOps > 0 as localSeqScanFileOps,
    localWriteFileOps > 0 as localWriteFileOps,
    remoteWriteFileOps > 0 as remoteWriteFileOps,
    filesCreated > 0 as filesCreated,
    profPersistentReadCache > 0 as profPersistentReadCache,
    profPersistentWriteCache > 0 as profPersistentWriteCache,
    profPersistentReadS3 > 0 as profPersistentReadS3,
    profPersistentWriteS3 > 0 as profPersistentWriteS3,
    profIntDataReadLocalSSD > 0 as profIntDataReadLocalSSD,
    profIntDataWriteLocalSSD > 0 as profIntDataWriteLocalSSD,
    profIntDataReadS3 > 0 as profIntDataReadS3,
    profIntDataWriteS3 > 0 as profIntDataWriteS3,
    profRemoteExtRead > 0 as profRemoteExtRead,
    profRemoteExtWrite > 0 as profRemoteExtWrite,
    profResWriteS3 > 0 as profResWriteS3,
    profFsMeta > 0 as profFsMeta,
    profDataExchangeNet > 0 as profDataExchangeNet,
    profDataExchangeMsg > 0 as profDataExchangeMsg,
    profControlPlaneMsg > 0 as profControlPlaneMsg,
    profOs > 0 as profOs,
    profMutex > 0 as profMutex,
    profSetup > 0 as profSetup,
    profSetupMesh > 0 as profSetupMesh,
    profTeardown > 0 as profTeardown,
    profScanRso > 0 as profScanRso,
    profXtScanRso > 0 as profXtScanRso,
    profProjRso > 0 as profProjRso,
    profSortRso > 0 as profSortRso,
    profFilterRso > 0 as profFilterRso,
    profResRso > 0 as profResRso,
    profDmlRso > 0 as profDmlRso,
    profHjRso > 0 as profHjRso,
    profBufRso > 0 as profBufRso,
    profFlatRso > 0 as profFlatRso,
    profBloomRso > 0 as profBloomRso,
    profAggRso > 0 as profAggRso,
    profBandRso > 0 as profBandRso,

    from 'snowset/part.*.parquet'
    """
    sf_features = duckdb.query(query).pl()
    sf_corr = sf_features.corr()
    sf_corr
    return query, sf_corr, sf_features


@app.cell
def _(sf_corr):
    import numpy as np
    import scipy.cluster.hierarchy as sch
    import seaborn as sns
    import matplotlib.pyplot as plt

    # Compute the correlation matrix
    corr_matrix = sf_corr.to_numpy()

    # Clean up numerical error
    for i in range(len(corr_matrix)):
        corr_matrix[i, i] = 1
    corr_matrix = (corr_matrix + corr_matrix.T) / 2

    # Create the distance matrix: 1 - |correlation|
    dist_matrix = 1 - np.abs(corr_matrix)

    # Perform hierarchical clustering on the symmetric distance matrix
    linkage = sch.linkage(sch.distance.squareform(dist_matrix), method='average')

    # Get the order of the features after clustering
    ordered_indices = sch.dendrogram(linkage, no_plot=True)['leaves']

    # Reorder the matrix using the ordered indices
    corr_matrix = corr_matrix[np.ix_(ordered_indices, ordered_indices)]

    # Step 4: Plot the reordered correlation matrix
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=False, cmap='coolwarm', vmin=-1, vmax=1, yticklabels=[sf_corr.columns[i] for i in ordered_indices])
    plt.title('Reordered Correlation Matrix with Hierarchical Clustering')
    plt.show()
    return (
        corr_matrix,
        dist_matrix,
        i,
        linkage,
        np,
        ordered_indices,
        plt,
        sch,
        sns,
    )


@app.cell
def _(duckdb):
    import altair as alt 

    sf_costs = duckdb.query(
    """
    select 
        case
            when profRemoteExtRead > 0 then 'Ingest'
            when profRemoteExtWrite > 0 then 'Export'
            when persistentReadBytesS3 + persistentReadBytesCache == 0 and persistentWriteBytesS3 > 0 then 'Ingest'
            when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 > 0 then 'Transformation'
            when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 == 0 then 'Read'
            else 'Other' end as "Query Type",
        sum(durationExec * warehouseSize) as "Cost",
    from 'snowset/part.*.parquet'
    group by all
    """
    ).df()
    sf_costs['Cost'] = sf_costs['Cost'] / sf_costs['Cost'].sum()

    alt.Chart(sf_costs).mark_bar(color='blue').encode(
        x='Cost', 
        y=alt.Y('Query Type', sort=["Ingest", "Transformation", "Read", "Export", "Other"])
    )
    return alt, sf_costs


@app.cell
def _(duckdb):
    redshift_sample = duckdb.query("select * from 'redset/provisioned/parts/*.parquet' using sample 1 percent (bernoulli)").df()
    redshift_sample
    return redshift_sample,


@app.cell
def _(duckdb):
    rs_sum = duckdb.query(
    """
    select 
        query_type, 
        sum(if(read_table_ids is not null, execution_duration_ms * cluster_size, 0)) as reads,
        sum(if(write_table_ids is not null, execution_duration_ms * cluster_size, 0)) as writes,
        sum(execution_duration_ms * cluster_size) as cost
    from 'redset/provisioned/parts/*.parquet'
    group by all
    order by cost desc
    """
    ).df()
    rs_sum['reads'] = rs_sum['reads'] / rs_sum['cost'].sum()
    rs_sum['writes'] = rs_sum['writes'] / rs_sum['cost'].sum()
    rs_sum['cost'] = rs_sum['cost'] / rs_sum['cost'].sum()
    rs_sum
    return rs_sum,


@app.cell
def _(alt, duckdb):
    redshift_costs = duckdb.query(
    """
    select 
        case when query_type in ('insert', 'copy', 'delete', 'update') then 'Ingest'
             when query_type in ('ctas') then 'Transformation'
             when query_type in ('select') then 'Read'
             when query_type in ('unload') then 'Export'
             when query_type in ('analyze', 'vacuum', 'other') then 'Other'
             else error(query_type) end as "Query Type",
        sum(execution_duration_ms * cluster_size) as "Cost"
    from 'redset/provisioned/parts/*.parquet'
    where cluster_size is not null
    group by all
    """
    ).df()
    redshift_costs['Cost'] = redshift_costs['Cost'] / redshift_costs['Cost'].sum()

    alt.Chart(redshift_costs).mark_bar(color='red').encode(
        x='Cost', 
        y=alt.Y('Query Type', sort=["Ingest", "Transformation", "Read", "Export", "Other"])
    )
    return redshift_costs,


@app.cell
def _(alt, duckdb):
    combined_costs = duckdb.query(
    """
    select 'Snowflake' as "Warehouse", *
    from sf_costs
    union all
    select 'Redshift' as "Warehouse", *
    from redshift_costs
    """
    ).df()

    alt.Chart(combined_costs).mark_bar().encode(
        color=alt.Color('Warehouse').scale(scheme='paired'),
        yOffset='Warehouse',
        x='Cost', 
        y=alt.Y('Query Type', sort=["Ingest", "Transformation", "Read", "Export", "Other"])
    ).properties(
        width=200,
        height=200,
    )
    return combined_costs,


@app.cell
def _(duckdb):

    sf_sizes = duckdb.query(
    """
    select warehouseSize * perServerCores as vCPU, count(*) as count 
    from 'snowset/part.*.parquet' 
    group by all 
    having vCPU >= 8 
    order by all"""
    ).df()

    alt.Chart(sf_sizes).mark_bar().encode(x='count:Q', y='vCPU:N')
    return alt, sf_sizes


@app.cell
def _(alt, duckdb):
    sf_scanned = duckdb.query(
    """
    select floor(log2((persistentReadBytesS3 + persistentReadBytesCache) / 1024 / 1024)) as log_mb_scanned, 2**log_mb_scanned as mb_scanned, count(*) as count 
    from 'snowset/part.*.parquet' 
    where persistentWriteBytesS3 = 0
    and profRemoteExtRead == 0
    and profRemoteExtWrite == 0
    and persistentReadBytesS3 + persistentReadBytesCache >= 1024*1024 
    group by all 
    order by all
    """
    ).df()
    sf_scanned['p'] = sf_scanned['count'] / sf_scanned['count'].sum()

    alt.Chart(sf_scanned).mark_bar().encode(x='mb_scanned:O', y='p:Q')
    return sf_scanned,


@app.cell
def _(alt, duckdb):
    rs_scanned = duckdb.query(
    """
    select floor(log2(mbytes_scanned)) as log_mb_scanned, 2**log_mb_scanned as mb_scanned, count(*) as count 
    from 'redset/provisioned/parts/*.parquet' 
    where query_type = 'select' 
    and mbytes_scanned >= 1
    and num_permanent_tables_accessed > 0
    group by all 
    order by all
    """
    ).df()
    rs_scanned['p'] = rs_scanned['count'] / rs_scanned['count'].sum()

    alt.Chart(rs_scanned).mark_bar().encode(x='mb_scanned:O', y='p:Q')
    return rs_scanned,


@app.cell
def _(alt, duckdb):
    combined_scanned = duckdb.query(
    """
    with combined as (
        select 'Snowflake' as warehouse, * from sf_scanned
        union all
        select 'Redshift' as warehouse, * from rs_scanned
    )
    select 
        * exclude (mb_scanned), 
        least(mb_scanned, 2**20) as mb_scanned,
        case when mb_scanned < 1024 then format('{:d} MB', mb_scanned::int)
            when mb_scanned < 1024*1024 then format('{:d} GB', (mb_scanned/1024)::int)
            else '1 TB+' end as label
    from combined
    """
    ).df()
    labels = {2**i: f"{2**i} MB" for i in range(0, 10)} | {2**i: f"{2**(i-10)} GB" for i in range(10,20)} | {2**20: f"1 TB+"}

    alt.Chart(combined_scanned).mark_bar().encode(
        color=alt.Color('warehouse').scale(scheme='paired').title('System'),
        xOffset='warehouse',
        x=alt.X('mb_scanned:O').title('Data Scanned').axis(labelExpr='datum.value < 1024 ? datum.value + " MB" : datum.value < 1024*1024 ? datum.value/1024 + " GB" : "1 TB+"'), 
        y=alt.Y('p:Q').title('Query Fraction')
    ).properties(
        width=400,
        height=200,
    )
    return combined_scanned, labels


@app.cell
def _(duckdb):
    duckdb.query(
    """
    select 'Redshift' as warehouse, quantile_cont(mbytes_scanned, [0.5, 0.999]) as q
    from 'redset/provisioned/parts/*.parquet' 
    where query_type = 'select' 
    and mbytes_scanned >= 1
    and num_permanent_tables_accessed > 0

    union all

    select 'Snowflake' as warehouse, quantile_cont((persistentReadBytesS3 + persistentReadBytesCache) / 1024 / 1024, [0.5, 0.999]) as q
    from 'snowset/part.*.parquet' 
    where persistentWriteBytesS3 = 0
    and profRemoteExtRead == 0
    and profRemoteExtWrite == 0
    and persistentReadBytesS3 + persistentReadBytesCache >= 1024*1024 
    """
    ).df()
    return


if __name__ == "__main__":
    app.run()
