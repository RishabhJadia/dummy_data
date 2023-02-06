if 'source_data_inventory' in formdata and formdata['source_data_inventory'] == 'inventory':
    qmgr_keys = {
        'INVENTORY': ['inv_acc_qmgr', 'inv_acc_host', 'inv_acc_service', 'inv_acc_contact'],
    }
    filter_type = 'INVENTORY'
    columns = ['qmgr.NAME', 'host.NAME']
    if formdata['query_by'] == 'qry_qmgr':
        inventory_service_data = self.get_inventory_service_data(all_qmgrs, flag='qmgr')
        inventory_service_df = pd.DataFrame(inventory_service_data)
        inv_data = {}
        if len(inventory_service_df):
            columns.extend(['service.NAME', 'contact.DATA'])
            inv_data = set(inventory_service_df['qmgr.NAME'].to_list())
            inventory_service_df = pd.DataFrame(inventory_service_data)
            inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
                .agg({'contact.DATA': ','.join,
                      **{key: 'first' for key in inventory_service_df.keys()
                         if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                        'contact.DATA']}}).reset_index()
            inventory_service_data = inventory_service_df.to_dict('records')
        is_all_qmgrs = [qmgr for qmgr in all_qmgrs if qmgr not in inv_data]
        if is_all_qmgrs:
            inventory_qmgr_data, errorneous_records = self.get_inventories_qmgr_data(is_all_qmgrs)
        else:
            inventory_qmgr_data, errorneous_records = [], []
        inventory_data = inventory_service_data + inventory_qmgr_data
    elif formdata['query_by'] == 'qry_host':
        inventory_service_data = self.get_inventory_service_data(all_qmgrs, flag='host')

        inventory_service_df = pd.DataFrame(inventory_service_data)
        inv_data = {}
        if len(inventory_service_df):
            columns.extend(['service.NAME', 'contact.DATA'])
            inv_data = set(inventory_service_df['host.NAME'].to_list())
            inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
                .agg({'contact.DATA': ','.join,
                      **{key: 'first' for key in inventory_service_df.keys()
                         if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                        'contact.DATA']}}).reset_index()
            inventory_service_data = inventory_service_df.to_dict('records')

        is_all_qmgrs = [qmgr for qmgr in all_qmgrs if qmgr not in inv_data]

        if is_all_qmgrs:
            inventory_qmgr_data, errorneous_records = self.get_inventories_host_data(is_all_qmgrs)
        else:
            inventory_qmgr_data, errorneous_records = [], []
        inventory_data = inventory_service_data + inventory_qmgr_data

    elif formdata['query_by'] == 'qry_service':
        inventory_data, errorneous_records = self.get_inventory_service_data(all_qmgrs, flag='service'), []
        inventory_service_df = pd.DataFrame(inventory_data)
        if len(inventory_service_df):
            columns.extend(['service.NAME', 'contact.DATA'])
            inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
                .agg({'contact.DATA': ','.join,
                      **{key: 'first' for key in inventory_service_df.keys()
                         if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                        'contact.DATA']}}).reset_index()
            inventory_data = inventory_service_df.to_dict('records')
    else:
        inventory_data, errorneous_records = self.get_inventory_service_data(all_qmgrs, flag='sealid'), []
        inventory_service_df = pd.DataFrame(inventory_data)
        if len(inventory_service_df):
            columns.extend(['service.NAME', 'contact.DATA'])
            inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
                .agg({'contact.DATA': ','.join,
                      **{key: 'first' for key in inventory_service_df.keys()
                         if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                        'contact.DATA']}}).reset_index()
            inventory_data = inventory_service_df.to_dict('records')
    op = [formdata[rec] for rec in qmgr_keys[filter_type] if rec in formdata and formdata[rec]]
    op = list(set(self.split_user_input(','.join(op))))
    inv_df = pd.DataFrame(inventory_data, columns=['qmgr.NAME', 'host.NAME', 'service.NAME', 'contact.DATA'] + op)
    query_result = inv_df.to_dict('records')

    return query_result, errorneous_records



1st way FOR MQ

if 'source_data_qmgr' in formdata and formdata['source_data_qmgr'] == 'qmgr':
    object = formdata['mq_qmgr_object']
    keys = {'channel': ['CHLTYPE', ['svrconn', 'sdr', 'rcvr', 'clussdr', 'clusrcvr']],
            'queue': ['QTYPE', ['local', 'remote', 'alias']]}
    object_data, error = (self.get_channels(all_qmgrs) if object == 'channel' 
                         else self.get_queues(all_qmgrs))
    op = [formdata[rec] for rec in keys[object][1] if rec in formdata and formdata[rec]]
    op = list(set(self.split_user_input(','.join(op))))
    df = pd.DataFrame(object_data, columns=['QMGR', keys[object][0], f"{object.upper()}" ] + op)
    query_result = df.to_dict('records')



2nd way FOR MQ
def process_qmgr_data(formdata, all_qmgrs, self):
    qmgr_keys = {'channel': ('CHLTYPE', ['svrconn', 'sdr', 'rcvr', 'clussdr', 'clusrcvr']),
                 'queue': ('QTYPE', ['local', 'remote', 'alias'])}
    object_type, (col_name, fields) = qmgr_keys[formdata['mq_qmgr_object']]
    op = [formdata[field] for field in fields if field in formdata and formdata[field]]
    op = list(set(self.split_user_input(','.join(op))))
    data, _ = (self.get_channels if object_type == 'channel' else self.get_queues)(all_qmgrs)
    return pd.DataFrame(data, columns=['QMGR', col_name, object_type.upper()] + op).to_dict('records')

if 'source_data_qmgr' in formdata and formdata['source_data_qmgr'] == 'qmgr':
    query_result = process_qmgr_data(formdata, all_qmgrs, self)


3rd way FOR MQ
def process_fields(formdata, fields):
    return list(set(self.split_user_input(','.join([formdata[field] for field in fields if field in formdata and formdata[field]]))))

def process_qmgr_data(formdata, all_qmgrs, self):
    qmgr_keys = {'channel': ('CHLTYPE', ['svrconn', 'sdr', 'rcvr', 'clussdr', 'clusrcvr']), 'queue': ('QTYPE', ['local', 'remote', 'alias'])}
    # object_type = formdata['mq_qmgr_object']
    #col_name, fields = qmgr_keys[object_type]
    object_type, (col_name, fields) = qmgr_keys[formdata['mq_qmgr_object']]
    data, errorneous_records = (self.get_channels if object_type == 'channel' else self.get_queues)(all_qmgrs)
    data_df =  pd.DataFrame(data, columns=['QMGR', col_name, object_type.upper()] + process_fields(formdata, fields)).to_dict('records')
    return data_df, errorneous_records

if 'source_data_qmgr' in formdata and formdata['source_data_qmgr'] == 'qmgr':
    query_result, errorneous_records = process_qmgr_data(formdata, all_qmgrs, self)




1st way for INVENTORY
if 'source_data_inventory' not in formdata or formdata['source_data_inventory'] != 'inventory':
    return

qmgr_data = []
columns = []
filter_type = 'INVENTORY'
qmgr_keys = {'INVENTORY': ['inv_acc_qmgr', 'inv_acc_host', 'inv_acc_service', 'inv_acc_contact']}
columns = ['qmgr.NAME', 'host.NAME']

if formdata['query_by'] == 'qry_qmgr':
    flag = 'qmgr'
elif formdata['query_by'] == 'qry_host':
    flag = 'host'
else:
    flag = 'service'
    columns.extend(['service.NAME', 'contact.DATA'])

inventory_service_data = self.get_inventory_service_data(all_qmgrs, flag=flag)
inventory_service_df = pd.DataFrame(inventory_service_data)
inv_data = set(inventory_service_df[f'{flag}.NAME'].to_list())
is_all_qmgrs = [qmgr for qmgr in all_qmgrs if qmgr not in inv_data]

if flag == 'qmgr':
    func = self.get_inventories_qmgr_data
elif flag == 'host':
    func = self.get_inventories_host_data
else:
    func = self.get_inventory_service_data
    inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
        .agg({'contact.DATA': ','.join,
              **{key: 'first' for key in inventory_service_df.keys()
                 if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                'contact.DATA']}}).reset_index()
    inventory_service_data = inventory_service_df.to_dict('records')

if is_all_qmgrs:
    inventory_qmgr_data, errorneous_records = func(is_all_qmgrs)
else:
    inventory_qmgr_data, errorneous_records = [], []

inventory_data = inventory_service_data + inventory_qmgr_data


2nd way
def process_inventory_data(self, formdata, all_qmgrs):
    if 'source_data_inventory' not in formdata or formdata['source_data_inventory'] != 'inventory':
        return None, None, None
    
    qmgr_keys = {'INVENTORY': ['inv_acc_qmgr', 'inv_acc_host', 'inv_acc_service', 'inv_acc_contact']}
    filter_type = 'INVENTORY'
    columns = ['qmgr.NAME', 'host.NAME']

    if formdata['query_by'] == 'qry_qmgr':
        data_func = self.get_inventory_service_data
        flag = 'qmgr'
    elif formdata['query_by'] == 'qry_host':
        data_func = self.get_inventories_host_data
        flag = 'host'
    elif formdata['query_by'] == 'qry_service':
        data_func = self.get_inventory_service_data
        flag = 'service'
    else:
        return None, None, None

    inventory_service_data = data_func(all_qmgrs, flag=flag)
    inventory_service_df = pd.DataFrame(inventory_service_data)
    inv_data = set(inventory_service_df[f'{flag}.NAME'].to_list()) if len(inventory_service_df) else set()
    
    is_all_qmgrs = [qmgr for qmgr in all_qmgrs if qmgr not in inv_data]
    if is_all_qmgrs:
        inventory_qmgr_data, errorneous_records = self.get_inventories_qmgr_data(is_all_qmgrs) if flag == 'qmgr' else self.get_inventories_host_data(is_all_qmgrs)
    else:
        inventory_qmgr_data, errorneous_records = [], []
    
    inventory_data = inventory_service_data + inventory_qmgr_data
    
    if len(inventory_service_df):
        columns.extend(['service.NAME', 'contact.DATA'])
        inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
            .agg({'contact.DATA': ','.join,
                  **{key: 'first' for key in inventory_service_df.keys()
                     if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                    'contact.DATA']}}).reset_index()
        inventory_data = inventory_service_df.to_dict('records')
    
    return filter_type, columns, inventory_data
