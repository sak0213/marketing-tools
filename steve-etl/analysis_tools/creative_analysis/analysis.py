import numpy
import pandas

def adjusted_cpa_view(source_df=None, pre_aggregated = True):
    """
    Outputs a dataframe with adjusted \n
    CPAs.\n\n

    Adjusted CPA is the CPA of your campaign\n
    if a specific row never existed. This offers\n
    an alternative view of the impact of an\n
    entry on a campaign's performance.\n\n

    Takes a dataframe of index | leads, spend.    
    """
    if 'leads' not in source_df.columns and 'spend' not in source_df.columns:
        print('Key columns not present. \nPlease check that required columns are in DF')

    if pre_aggregated == True:        
        total_leads = source_df['leads'].sum()
        total_spend = source_df['spend'].sum()
        total_cpa = total_spend / total_leads

        source_df['Adjusted CPA'] = (total_spend - source_df['spend']) / (total_leads - source_df['leads'])
        source_df['Percent Change'] = source_df['Adjusted CPA'] / total_cpa -1

        print(f'Total average CPA is ${format(total_cpa,".2f")}\n')
        print(source_df.head())
        print('------------------------------------------------------------\n\n')

def explode_to_samples(aggregated_df=None, group_field=None, N_field=None, result_field=None):
    """
    This takes a summarized DF and explodes it\n
    into synthetic log-level data. \n
    This approach helps to simplify modeling where applicable.\n
    Results MUST count a binary event for each N
    """
    group_list = []
    result_list = []

    for i in aggregated_df.index:
        n = aggregated_df.iloc[i][N_field]
        results = aggregated_df.iloc[i][result_field]
        negative_results = n-results
        positive_results = n-negative_results
        for sample in range(n):
            group_list.append(aggregated_df.iloc[i][group_field])
        for negative in range(negative_results):
            result_list.append(0)
        for positive in range(positive_results):
            result_list.append(1)

    output_df = pandas.DataFrame(data={
        group_field:group_list,
        'results':result_list
    })

    return output_df
