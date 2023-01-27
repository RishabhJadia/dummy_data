
def _get_validate_qmgr(self, formdata):
    qmgrs_list = []
    errorneous_resp = {"errors": ''}
    if 'source_data_qmgr' in formdata and formdata['source_data_qmgr'] == 'qmgr':
        env, qmgr = formdata['mq_environment'], formdata['mq_qmgr']
        region = 'mq_region' in formdata and formdata['mq_region'] or ''

        all_qmgrs = self.get_qmgrs(env.upper() if env != 'other' else None)
        if isinstance(all_qmgrs, list):
            qmgrs_list = qmgr.split(',') if env == 'other' else [rec for rec in all_qmgrs if rec[:2] == region.upper()]
            invalid_qmgr = [qmgr for qmgr in qmgrs_list if qmgr not in all_qmgrs]
            errorneous_resp['errors'] = f'Invalid QMGR - {invalid_qmgr}' if invalid_qmgr else ''
        else:
            errorneous_resp['errors'] = all_qmgrs
    elif 'source_data_inventory' in formdata and formdata['source_data_inventory'] == 'inventory':
        if 'query_by' in formdata:
            query_by = formdata['query_by']
            qmgr = formdata['inv_qmgr']
            qmgrs_list = qmgr.split(',')
            all_qmgrs = None
            invalid_qmgrs = []
            if query_by == 'qry_qmgr':
                env = formdata['inv_environment']
                region = 'inv_region' in formdata and formdata['inv_region'] or ''
                all_qmgrs = self.get_qmgrs(env.upper() if env != 'other' else None)
                if env != 'other':
                    qmgrs_list = [rec for rec in all_qmgrs if rec[:2] == region.upper()]
            elif query_by == 'qry_host':
                all_qmgrs = self.get_hosts()
            elif query_by == 'qry_service':
                all_qmgrs = self.get_services()
            elif query_by == 'qry_sealid':
                all_qmgrs = self.get_sealids()
                qmgrs_list = [int(qmgr) for qmgr in qmgrs_list]

            if isinstance(all_qmgrs, list):
                invalid_qmgrs = [qmgr for qmgr in qmgrs_list if qmgr not in all_qmgrs]
                errorneous_resp['errors'] = f'Invalid {query_by.replace("qry_", "")}s - {invalid_qmgrs}' if invalid_qmgrs else ''
            else:
                errorneous_resp['errors'] = all_qmgrs
    
    return qmgrs_list, errorneous_resp



def mqsearch_formdata(self, formdata, mail_from, mail_to):
    results = {"message": "No Result Found"}
    all_qmgrs, errorneous_resp = self._get_validate_qmgr(formdata)
    if errorneous_resp['errors']:
        return errorneous_resp  # early return hatana h last m email ke saath hi invalid data kr k send krna h

    qmgr_object_resp = self._get_qmgr_object(formdata, all_qmgrs)
    obj_type = self._get_object_type(formdata)
    if obj_type:
        responses = self._make_response(qmgr_object_resp, obj_type)
        results = self._send_email(formdata, responses, mail_from, mail_to)
    return results
