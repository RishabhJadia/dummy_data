
def _get_valid_usr_inputs(self, formdata):
    qmgrs_list, errorneous_resp = [], {"errors": ''}
    if 'source_data_qmgr' in formdata and formdata['source_data_qmgr'] == 'qmgr':
        qmgrs_list, errorneous_resp = self._validate_mq_input(formdata)
    elif 'source_data_inventory' in formdata and formdata['source_data_inventory'] == 'inventory':
        qmgrs_list, errorneous_resp = self._validate_inventory_input(formdata)

    return qmgrs_list, errorneous_resp

def _qmgrs_valid(self, qmgr, env, region):
    all_qmgrs = self.get_qmgrs(env.upper() if env != 'other' else None)
    qmgrs_list = qmgr.split(',') if env == 'other' else [rec for rec in all_qmgrs if rec[:2] == region.upper()]
    return qmgrs_list, all_qmgrs  

def _validate_inputs(self, env, qmgrs_list, all_qmgrs):
    errorneous_resp = {"errors": ''}
    if env == 'other':
        invalid_qmgr = [qmgr for qmgr in qmgrs_list if qmgr not in all_qmgrs]
        errorneous_resp['errors'] = f'Invalid QMGR - {invalid_qmgr}' if invalid_qmgr else ''

        qmgrs_list = [qmgr for qmgr in qmgr_list if qmgr not in invalid_qmgr] # returns only valid qmgrs

    return qmgrs_list, errorneous_resp

def _validate_mq_input(self, formdata):
    env, qmgr = formdata['mq_environment'], formdata['mq_qmgr']
    region = 'mq_region' in formdata and formdata['mq_region'] or ''

    # all_qmgrs = self.get_qmgrs(env.upper() if env != 'other' else None)
    # qmgrs_list = qmgr.split(',') if env == 'other' else [rec for rec in all_qmgrs if rec[:2] == region.upper()]

    qmgrs_list, all_qmgrs = self._qmgrs_valid(qmgr, env, region)

    qmgrs_list, errorneous_resp = self._validate_inputs(env, qmgrs_list, all_qmgrs)
    return qmgrs_list, errorneous_resp


def _validate_inventory_input(self, formdata):
    query_by = formdata['query_by']
    qmgr = formdata['inv_qmgr']
    qmgrs_list = qmgr.split(',')
    all_qmgrs = None
    env = 'other'
    if query_by == 'qry_qmgr':
        env = formdata['inv_environment']
        regional = 'inv_region' in formdata and formdata['inv_region'] or ''

        # all_qmgrs = self.get_qmgrs(env.upper() if env != 'other' else None)
        # qmgrs_list = qmgrs_list if env == 'other' else [rec for rec in all_qmgrs if rec[:2] == region.upper()]
        qmgrs_list, all_qmgrs = self._qmgrs_valid(qmgr, env, region)
    elif query_by == 'qry_host':
        all_qmgrs = self.get_hosts()
    elif query_by == 'qry_service':
        all_qmgrs = self.get_services()
    elif query_by == 'qry_sealid':
        all_qmgrs = self.get_sealids()
        qmgrs_list = [int(qmgr) for qmgr in qmgrs_list]


    qmgrs_list, errorneous_resp = self._validate_inputs(env, qmgrs_list, all_qmgrs)
    return qmgrs_list, errorneous_resp

def _get_qmgr_object(self, formdata, all_qmgrs):
    pass

def mqsearch_formdata(self, formdata, mail_from, mail_to):
    results = {"message": "No Result Found"}
    all_qmgrs, errorneous_resp = self._get_valid_usr_inputs(formdata)
    if errorneous_resp['errors']:
        return errorneous_resp  # early return hatana h last m email ke saath hi invalid data kr k send krna h

    qmgr_object_resp = self._get_qmgr_object(formdata, all_qmgrs)
    # obj_type = self._get_object_type(formdata)
    # responses = self._make_response(qmgr_object_resp, obj_type)
    responses = self._make_response(qmgr_object_resp)
    results = self._send_email(formdata, responses, mail_from, mail_to)
    return results


# def _get_object_type(self, formdata):
    # formdata = formdata.get('mq_qmgr_object') or formdata.get('source_data_inventory')
    # return formdata

def _make_response(self, qmgr_object_resp, obj_type):
    # code to make response
    return responses

def _send_email(self, formdata, responses, mail_from, mail_to):
    # code to send email
    return results

