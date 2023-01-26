from datetime import date, datetime, timedelta
import itertools
import logging
# import sys
import requests
import pandas as pd
import numpy as np
import asyncio
import os
import concurrent.futures

# from commons.templates.message_templates import ReturnMsgs
# from utils.utilities import convert_json_to_html
from utils.utilities import get_fqdn_of_qmgr, load_json_config_file, call_multi_thread_executor
# from services.emp import EMP
# from services.gaia import get_app_uri
from services.mqadmin import MQAdmin
from services.opsportal_email import EMail
# from services.plm import PlmClient
from services.um_inventory import UMInvClient

# from services.uminv_datamassage import UmInventoryMassage


logger = logging.getLogger(__name__)
MQSEARCHENGINE_LOGGER = "SERVICE_LOGGER:mqsearch_engine:"
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))


# pylint: disable=too-many-arguments


class MQSearchEngine:

    def __init__(self, tokens=None):
        # self.blackout = EMP(tokens)
        self.email = EMail()
        self.mqadminview = MQAdmin(tokens)
        self.uminv = UMInvClient(tokens)
        # self.uminv_datamassage = UmInventoryMassage(tokens)
        # self.plmclient = PlmClient(tokens)
        self.today = datetime.now().strftime("%Y-%m-%d-%H%M%S")

    def get_qmgrs(self, env='*'):
        """
        :param env: PROD,UAT,DEV. default=*
        :rtype: list of dictionary eg. [{'qmgr.ENVIRONMENT': 'PROD', 'qmgr.NAME': 'NATM001', 'qmgr.STATUS': 'LIVE'},]
        """
        # qmgr_by_env = self.uminv.get_queue_manager_by_env(env)
        qmgr_by_env = self.uminv.get_inventory_qmgr_data_by_env(env)
        qmgrs = {"message": "Errorneous response from MQADMIN API"}
        if isinstance(qmgr_by_env, list):
            qmgrs = [qmgr['qmgr.NAME'] for qmgr in qmgr_by_env]
        return qmgrs

    def get_hosts(self, env='*'):
        """
        :param env: PROD,UAT,DEV. default=*
        :rtype: list of dictionary eg. [{'qmgr.ENVIRONMENT': 'PROD', 'qmgr.NAME': 'NATM001', 'qmgr.STATUS': 'LIVE'},]
        """
        # qmgr_by_env = self.uminv.get_queue_manager_by_env(env)
        hosts_details = self.uminv.get_inventory_hosts_data_by_env(env)
        qmgrs = {"message": "Errorneous response from MQADMIN API"}
        if isinstance(hosts_details, list):
            qmgrs = [host['host.NAME'] for host in hosts_details]
        return qmgrs

    def get_services(self):
        """
        :param env: PROD,UAT,DEV. default=*
        :rtype: list of dictionary eg. [{'qmgr.ENVIRONMENT': 'PROD', 'qmgr.NAME': 'NATM001', 'qmgr.STATUS': 'LIVE'},]
        """
        # service_details = self.uminv.get_inventory_service_data_by_custom_search()
        service_details = self.uminv.get_inventory_services_data()
        qmgrs = {"message": "Errorneous response from MQADMIN API"}
        if isinstance(service_details, list):
            qmgrs = [service['service.NAME'] for service in service_details]
        return qmgrs

    def get_sealids(self):
        """
        :param env: PROD,UAT,DEV. default=*
        :rtype: list of dictionary eg. [{'qmgr.ENVIRONMENT': 'PROD', 'qmgr.NAME': 'NATM001', 'qmgr.STATUS': 'LIVE'},]
        """
        # service_sealid_details = self.uminv.get_inventory_service_data_by_custom_search()
        service_sealid_details = self.uminv.get_inventory_sealids_data()
        qmgrs = {"message": "Errorneous response from MQADMIN API"}
        if isinstance(service_sealid_details, list):
            qmgrs = [service_sealid['service.SEAL_ID'] for service_sealid in service_sealid_details]
        return qmgrs

    def get_channels(self, qmgrs):
        channel_data = call_multi_thread_executor(self.mqadminview.get_channels_with_qmgr, qmgrs, max_workers=50,
                                                  job_name="mqsearch_get_channels")
        # breakpoint()
        errorneous_records = [rec for rec in channel_data if isinstance(rec, dict)]
        success_records = [rec1 for rec1 in channel_data if isinstance(rec1, list)]
        # breakpoint()
        channel_data = list(itertools.chain(*success_records))
        return channel_data, errorneous_records

    def get_queues(self, qmgrs):
        queue_data = call_multi_thread_executor(self.mqadminview.get_queues_with_qmgr, qmgrs, max_workers=50,
                                                job_name="mqsearch_get_queues")
        errorneous_records = [rec for rec in queue_data if isinstance(rec, dict)]
        success_records = [rec1 for rec1 in queue_data if isinstance(rec1, list)]
        queue_data = list(itertools.chain(*success_records))
        # breakpoint()
        return queue_data, errorneous_records

    # def get_inventories_qmgr_data(self, all_qmgrs, formdata):
    def get_inventories_qmgr_data(self, all_qmgrs):
        # if formdata['inv_environment'] == 'other':
        errorneous_records = []
        inventory_resp = self.uminv.get_inventory_data_by_qmgrs(','.join(all_qmgrs))
        # else:
        #     inventory_resp = self.uminv.get_inventory_qmgr_data_by_env(formdata['inv_environment'].upper())
        return inventory_resp, errorneous_records

    def get_inventories_host_data(self, all_qmgrs):
        # if formdata['inv_environment'] == 'other':
        errorneous_records = []
        inventory_resp = self.uminv.get_hosts_details_by_hostname(','.join(all_qmgrs))
        # else:
        #     inventory_resp = self.uminv.get_inventory_qmgr_data_by_env(formdata['inv_environment'].upper())
        return inventory_resp, errorneous_records

    def get_inventory_service_data(self, all_qmgrs, flag):
        # if formdata['inv_environment'] == 'other':
        # errorneous_records = []
        inventory_resp = self.uminv.get_inventory_service_data_by_custom_search(','.join(all_qmgrs), flag)
        # if not inventory_resp:
        #     errorneous_records = []
        # else:

        # breakpoint()
        # else:
        #     inventory_resp = self.uminv.get_inventory_qmgr_data_by_env(formdata['inv_environment'].upper())
        return inventory_resp

    def get_inventory_contact_data(self, all_qmgrs, flag):
        # if formdata['inv_environment'] == 'other':
        # errorneous_records = []
        inventory_resp = self.uminv.inventory_contact_data_by_custom_search(','.join(all_qmgrs), flag)
        if not inventory_resp:
            inventory_resp = []
        # else:

        # breakpoint()
        # else:
        #     inventory_resp = self.uminv.get_inventory_qmgr_data_by_env(formdata['inv_environment'].upper())
        return inventory_resp

    # def get_channels(self, qmgr):  # using async
    #     channel_data = self.mqadminview.get_channels(qmgr)
    #     breakpoint()
    #     _ = [channel.update({'QMGRR': qmgr}) for channel in channel_data]
    #     return channel_data

    # def mqsearch_channel_type_dynamic_query(self, channels_data, **kwargs):
    @staticmethod
    def mqsearch_channel_type_dynamic_query(formdata, query_data, type_info, filter_type=None, columns=None):
        if isinstance(query_data, list) and query_data:
            # breakpoint()
            df = pd.DataFrame(query_data)
            qmgr_keys = {
                'CHLTYPE': ['svrconn', 'sdr', 'rcvr', 'clussdr', 'clusrcvr'],
                'QTYPE': ['local', 'remote', 'alias'],
                'INVENTORY': ['inv_acc_qmgr', 'inv_acc_host', 'inv_acc_service', 'inv_acc_contact']
                # 'INVENTORY': ['inventory']
            }
            # final_records = {}
            final_records = []
            # breakpoint()

            if 'source_data_qmgr' in formdata and formdata[
                'source_data_qmgr'] == 'qmgr' and 'source_data_inventory' in formdata and formdata[
                'source_data_inventory'] == 'inventory':
                dic_data = []
                for qmgr_key, qmgr_value in qmgr_keys.items():
                    if qmgr_key != 'INVENTORY':
                        filtered_data = {value: [item for item in query_data for key, val in item.items() if val == value.upper()] for value in qmgr_value}
                        dic_data.append(filtered_data)
                # breakpoint()
            else:
                for qmgr_key in qmgr_keys[filter_type]:
                    # breakpoint()
                    if qmgr_key in type_info and type_info[qmgr_key]:
                        if filter_type != 'INVENTORY':
                            filtered_df = df.loc[df[filter_type] == qmgr_key.upper()]
                        else:
                            filtered_df = df

                        # filtered_df = df.loc[df[filter_type] == qmgr_key.upper()]

                        # breakpoint()
                        # https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python
                        # filtered_df = filtered_df.drop_duplicates()

                        if type_info[qmgr_key][0] == 'ALL':
                            # columns wala logic rearrrange k liye likhna h
                            # results = filtered_df.to_dict('records')

                            query_result = pd.DataFrame(filtered_df)
                            shuffled_columns = query_result[columns]
                            query_result = pd.concat([shuffled_columns, query_result.drop(columns=columns)], axis=1)
                            # breakpoint()
                            query_result = query_result.drop_duplicates()
                            results = query_result.to_dict('records')
                        else:
                            # results = pd.DataFrame(filtered_df, columns=['QMGR'] + type_info[qmgr_key])
                            results = pd.DataFrame(filtered_df, columns=columns + type_info[qmgr_key])
                            # breakpoint()
                            results = results.drop_duplicates()
                            results = results.to_dict('records')
                        final_records.append(results)
                        # final_records[qmgr_key.upper()] = results
                    else:
                        "PUT LOGGER"

            '''
            df = pd.DataFrame(channels_data)
            # final_records = {}
            final_records = []
            if kwargs['svrconn']:
                # breakpoint()
                svrconn = df.loc[df["CHLTYPE"] == "SVRCONN"]
                if kwargs['svrconn'][0] == 'ALL':
                    svrconn_records = svrconn.to_dict('records')
                else:
                    svrconn_records = pd.DataFrame(svrconn, columns=kwargs['svrconn'] + ['QMGR'])
                    svrconn_records = svrconn_records.to_dict('records')
                # final_records['SVRCONN'] = svrconn_records
                final_records.append(svrconn_records)
            else:
                "PUT LOGGER
            '''
            # breakpoint()
            final_records = list(itertools.chain(*final_records))
            # breakpoint()

            # print({k: len(v) for k, v in final_records.items()})

        else:
            final_records = []
            print("NO DATA FOUND")
        return final_records

    # async def _run_in_executor(self, loop, executor, qmgrs_list):
    #     breakpoint()
    #     tasks = [loop.run_in_executor(executor, self.get_channels, qmgr) for qmgr in qmgrs_list]
    #     results = await asyncio.gather(*tasks, return_exceptions=True)
    #     breakpoint()
    #     results = list(itertools.chain(*results))
    #     return results
    #
    # async def start_async_process(self, qmgrs_list, formdata):
    #     with concurrent.futures.ThreadPoolExecutor() as executor:
    #         loop = asyncio.get_event_loop()
    #         responses = await self._run_in_executor(loop, executor, qmgrs_list)
    #         svrconn = formdata['svrconn'].replace(' ', '').split(',') if formdata['svrconn'] else ''
    #         sdr = formdata['sdr'].replace(' ', '').split(',') if formdata['sdr'] else ''
    #         rcvr = formdata['rcvr'].replace(' ', '').split(',') if formdata['rcvr'] else ''
    #         clussdr = formdata['clussdr'].replace(' ', '').split(',') if formdata['clussdr'] else ''
    #         clusrcvr = formdata['clusrcvr'].replace(' ', '').split(',') if formdata['clusrcvr'] else ''
    #         breakpoint()
    #         query_result = self.mqsearch_channel_type_dynamic_query(channels_data=responses, svrconn=svrconn,
    #                                                                 sdr=sdr, rcvr=rcvr, clussdr=clussdr,
    #                                                                 clusrcvr=clusrcvr)

    def _get_validate_qmgr(self, formdata):
        # if qmgr and inventory:
        #     qmgr ka QMGR, ENV, REGION send kro
        # elif qmgr:
        #     qmgr ka QMGR, ENV, REGION send kro
        # elif inventory:
        #     inventory ka QMGR, ENV, REGION send kro

        qmgrs_list = []
        errorneous_resp = {}

        if 'source_data_qmgr' in formdata and formdata[
            'source_data_qmgr'] == 'qmgr' and 'source_data_inventory' in formdata and formdata[
            'source_data_inventory'] == 'inventory':
            errorneous_resp = {"errors": ''}
            env, qmgr = formdata['mq_environment'], formdata['mq_qmgr']
            region = 'mq_region' in formdata and formdata['mq_region'] or ''
            if env == 'other':
                all_qmgrs = self.get_qmgrs()
                qmgrs_list = qmgr.split(',')
                if isinstance(all_qmgrs, list):
                    invalid_qmgr = [qmgr for qmgr in qmgrs_list if qmgr not in all_qmgrs]
                    errorneous_resp['errors'] = f'Invalid QMGR - {invalid_qmgr}' if invalid_qmgr else ''
            else:
                all_qmgrs = self.get_qmgrs(env.upper())
                # breakpoint()
                qmgrs_list = [rec for rec in all_qmgrs if rec[:2] == region.upper()]
            if not isinstance(all_qmgrs, list):
                errorneous_resp['errors'] = all_qmgrs
        elif 'source_data_qmgr' in formdata and formdata['source_data_qmgr'] == 'qmgr':
            errorneous_resp = {"errors": ''}
            # env, qmgr = formdata['mq_environment'], formdata['qmgr']
            env, qmgr = formdata['mq_environment'], formdata['mq_qmgr']
            region = 'mq_region' in formdata and formdata['mq_region'] or ''
            if env == 'other':
                all_qmgrs = self.get_qmgrs()
                qmgrs_list = qmgr.split(',')
                if isinstance(all_qmgrs, list):
                    invalid_qmgr = [qmgr for qmgr in qmgrs_list if qmgr not in all_qmgrs]
                    errorneous_resp['errors'] = f'Invalid QMGR - {invalid_qmgr}' if invalid_qmgr else ''
            else:
                all_qmgrs = self.get_qmgrs(env.upper())
                # breakpoint()
                qmgrs_list = [rec for rec in all_qmgrs if rec[:2] == region.upper()]
            if not isinstance(all_qmgrs, list):
                errorneous_resp['errors'] = all_qmgrs
        elif 'source_data_inventory' in formdata and formdata['source_data_inventory'] == 'inventory':
            errorneous_resp = {"errors": ''}
            if 'query_by' in formdata and formdata['query_by'] == 'qry_qmgr':
                env, qmgr = formdata['inv_environment'], formdata['inv_qmgr']
                region = 'inv_region' in formdata and formdata['inv_region'] or ''
                if env == 'other':
                    all_qmgrs = self.get_qmgrs()
                    qmgrs_list = qmgr.split(',')
                    if isinstance(all_qmgrs, list):
                        invalid_qmgr = [qmgr for qmgr in qmgrs_list if qmgr not in all_qmgrs]
                        errorneous_resp['errors'] = f'Invalid QMGRs - {invalid_qmgr}' if invalid_qmgr else ''
                else:
                    all_qmgrs = self.get_qmgrs(env.upper())
                    # breakpoint()
                    qmgrs_list = [rec for rec in all_qmgrs if rec[:2] == region.upper()]
            elif 'query_by' in formdata and formdata['query_by'] == 'qry_host':
                hosts = formdata['inv_qmgr']
                all_qmgrs = self.get_hosts()
                qmgrs_list = hosts.split(',')
                if isinstance(all_qmgrs, list):
                    invalid_qmgr = [qmgr for qmgr in qmgrs_list if qmgr not in all_qmgrs]
                    errorneous_resp['errors'] = f'Invalid HOSTs - {invalid_qmgr}' if invalid_qmgr else ''
            elif 'query_by' in formdata and formdata['query_by'] == 'qry_service':
                services = formdata['inv_qmgr']
                all_qmgrs = self.get_services()
                qmgrs_list = services.split(',')
                if isinstance(all_qmgrs, list):
                    invalid_qmgr = [qmgr for qmgr in qmgrs_list if qmgr not in all_qmgrs]
                    errorneous_resp['errors'] = f'Invalid SERVICEs - {invalid_qmgr}' if invalid_qmgr else ''
            else:
                sealids = formdata['inv_qmgr']
                all_qmgrs = self.get_sealids()
                qmgrs_list = sealids.split(',')
                if isinstance(all_qmgrs, list):
                    invalid_qmgr = [qmgr for qmgr in qmgrs_list if int(qmgr) not in all_qmgrs]
                    errorneous_resp['errors'] = f'Invalid SEAL IDs - {invalid_qmgr}' if invalid_qmgr else ''

            if not isinstance(all_qmgrs, list):
                errorneous_resp['errors'] = all_qmgrs
        # breakpoint()
        return qmgrs_list, errorneous_resp

    def _get_qmgr_object(self, formdata, all_qmgrs):
        # breakpoint()
        errorneous_records = {"errors": ""}
        query_result = ''

        if 'source_data_qmgr' in formdata and formdata[
            'source_data_qmgr'] == 'qmgr' and 'source_data_inventory' in formdata and formdata[
            'source_data_inventory'] == 'inventory':
            qmgr_data = []
            columns = []
            filter_type = ''
            qmgr_keys = {
                'CHLTYPE': ['svrconn', 'sdr', 'rcvr', 'clussdr', 'clusrcvr'],
                'QTYPE': ['local', 'remote', 'alias'],
                # 'INVENTORY': ['inventory'],
                'INVENTORY': ['inv_acc_qmgr', 'inv_acc_host', 'inv_acc_service', 'inv_acc_contact']

            }

            # inventory_data, errorneous_records = self.get_inventories_qmgr_data(all_qmgrs)

            inventory_service_data = self.get_inventory_service_data(all_qmgrs, flag='qmgr')
            # breakpoint()
            inventory_service_df = pd.DataFrame(inventory_service_data)
            inv_data = {}
            # if inventory_service_data:
            if len(inventory_service_df):
                # columns.extend(['service.NAME', 'contact.DATA'])
                inv_data = set(inventory_service_df['qmgr.NAME'].to_list())
                inventory_service_df = pd.DataFrame(inventory_service_data)
                inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
                    .agg({'contact.DATA': ','.join,
                          **{key: 'first' for key in inventory_service_df.keys()
                             if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                            'contact.DATA']}}).reset_index()
                inventory_service_data = inventory_service_df.to_dict('records')
            # breakpoint()

            is_all_qmgrs = [qmgr for qmgr in all_qmgrs if qmgr not in inv_data]
            if is_all_qmgrs:
                inventory_qmgr_data, errorneous_records = self.get_inventories_qmgr_data(is_all_qmgrs)
            else:
                inventory_qmgr_data, errorneous_records = [], []
            # breakpoint()
            inventory_data = inventory_service_data + inventory_qmgr_data

            # breakpoint()

            columns_to_shuffle = ['qmgr.NAME', 'host.NAME', 'service.NAME', 'contact.DATA']
            op = [formdata[rec] for rec in qmgr_keys['INVENTORY'] if rec in formdata and formdata[rec]]
            op = list(set(self.split_user_input(','.join(op))))
            # breakpoint()
            inv_results = pd.DataFrame(inventory_data, columns=columns_to_shuffle + op)
            rename_column = {'qmgr.NAME': 'QMGR', 'host.NAME': 'HOST'}
            inv_results.rename(columns=rename_column, inplace=True)
            aa = inv_results.to_dict('records')
            # breakpoint()



            # inv_results = pd.DataFrame(inventory_data)
            # rename_column = {'qmgr.NAME': 'QMGR', 'host.NAME': 'HOST'}
            # inv_results.rename(columns=rename_column, inplace=True)
            # final_records = []
            # for qmgr_key in qmgr_keys['INVENTORY']:  #'INVENTORY': ['inv_acc_qmgr', 'inv_acc_host', 'inv_acc_service', 'inv_acc_contact']
            #     if qmgr_key in formdata and formdata[qmgr_key]:
            #         # filtered_df = inv_results.loc[inv_results['INVENTORY'] == qmgr_key.upper()]
            #         filtered_df = inv_results
            #         # breakpoint()
            #         if self.split_user_input(formdata[qmgr_key])[0] == 'ALL':
            #             results = pd.DataFrame(filtered_df, columns=['QMGR', 'HOST', 'service.NAME', 'contact.DATA'] + self.split_user_input(formdata[qmgr_key]))
            #             results = results.to_dict('records')
            #         else:
            #             results = pd.DataFrame(filtered_df, columns=['QMGR', 'HOST', 'service.NAME', 'contact.DATA'] + self.split_user_input(formdata[qmgr_key]))
            #             results = results.to_dict('records')
            #         final_records.append(results)
            # inv_results = list(itertools.chain(*final_records))
            # inv_results = pd.DataFrame(inv_results).drop_duplicates()


            if formdata['mq_qmgr_object'] == 'channel':
                columns_to_shuffle = ['QMGR', 'HOST', 'CHLTYPE', 'CHANNEL', 'service.NAME', 'contact.DATA']
                qmgr_data, errorneous_records = self.get_channels(all_qmgrs)
                op = [formdata[rec] for rec in qmgr_keys['CHLTYPE'] if rec in formdata and formdata[rec]]
                op = list(set(self.split_user_input(','.join(op))))
                chl_df = pd.DataFrame(qmgr_data, columns=['QMGR', 'CHLTYPE', 'CHANNEL'] + op)
                bb = chl_df.to_dict('records')

                # breakpoint()


                # columns = ['QMGR', 'CHLTYPE', 'CHANNEL']
                final_records = []
                # chl_df = pd.DataFrame(qmgr_data)
                # for qmgr_key in qmgr_keys['CHLTYPE']:  #'CHLTYPE': ['svrconn', 'sdr', 'rcvr', 'clussdr', 'clusrcvr']
                #     if qmgr_key in formdata and formdata[qmgr_key]:
                #         breakpoint()
                #         filtered_df = chl_df.loc[chl_df['CHLTYPE'] == qmgr_key.upper()]
                #         if self.split_user_input(formdata[qmgr_key])[0] == 'ALL':
                #             results = filtered_df.to_dict('records')
                #         else:
                #             results = pd.DataFrame(filtered_df, columns=columns + self.split_user_input(formdata[qmgr_key]))
                #             results = results.to_dict('records')
                #         final_records.append(results)
                # qmgr_records = list(itertools.chain(*final_records))
                # breakpoint()
                # df1 = pd.DataFrame(qmgr_records).drop_duplicates()
            elif formdata['mq_qmgr_object'] == 'queue':
                columns_to_shuffle = ['QMGR', 'HOST', 'QTYPE', 'QUEUE', 'service.NAME', 'contact.DATA']
                # columns_to_shuffle.insert(2, "QTYPE")
                # columns_to_shuffle.insert(3, "QUEUE")

                qmgr_data, errorneous_records = self.get_queues(all_qmgrs)
                op = [formdata[rec] for rec in qmgr_keys['QTYPE'] if rec in formdata and formdata[rec]]
                op = list(set(self.split_user_input(','.join(op))))
                chl_df = pd.DataFrame(qmgr_data, columns=['QMGR', 'QTYPE', 'QUEUE'] + op)
                bb = chl_df.to_dict('records')

                # columns = ['QMGR', 'QTYPE', 'QUEUE']
                # q_df = pd.DataFrame(qmgr_data)
                # final_records = []
                # for qmgr_key in qmgr_keys['QTYPE']:
                #     if qmgr_key in formdata and formdata[qmgr_key]:
                #         filtered_df = q_df.loc[q_df['QTYPE'] == qmgr_key.upper()]
                #         # breakpoint()
                #         if self.split_user_input(formdata[qmgr_key])[0] == 'ALL':
                #             # columns wala logic rearrrange k liye likhna h
                #             results = filtered_df.to_dict('records')
                #         else:
                #             results = pd.DataFrame(filtered_df, columns=columns + self.split_user_input(formdata[qmgr_key]))
                #             results = results.to_dict('records')
                #         final_records.append(results)
                # q_records = list(itertools.chain(*final_records))
                # df1 = pd.DataFrame(q_records).drop_duplicates()

            # 1st way
            # if formdata['mq_qmgr_object'] == 'channel':
            #     columns_to_shuffle = ['QMGR', 'HOST', 'CHLTYPE', 'CHANNEL', 'service.NAME', 'contact.DATA']
            #     # columns_to_shuffle = ['QMGR', 'HOST','HOST_STATUS',  'CHLTYPE', 'CHANNEL']
            # else:
            #     columns_to_shuffle = ['QMGR', 'HOST', 'QTYPE', 'QUEUE', 'service.NAME', 'contact.DATA']
            #     # columns_to_shuffle = ['QMGR', 'HOST', 'HOST_STATUS', 'QTYPE', 'QUEUE']

            # query_result = pd.concat([df1, inv_results])
            # breakpoint()
            # shuffled_columns = query_result[columns_to_shuffle]
            # query_result = pd.concat([shuffled_columns, query_result.drop(columns=columns_to_shuffle)], axis=1)
            # breakpoint()
            # query_result = query_result.to_dict('records')
            #

            # 2nd way
            # breakpoint()


            # aa = inv_results.to_dict('records')
            # bb = df1.to_dict('records')


            # cc = []
            # breakpoint()
            # for rec in aa:
            #     for rec1 in bb:
            #         if rec1['QMGR'] == rec['QMGR']:
            #             cc.append({
            #                 **rec1,
            #                 **rec
            #             })
            # breakpoint()
            # cc = aa + bb
            # for rec in aa:
            #     rec[''] =
            # breakpoint()
            cc = [{**rec, **rec1} for rec in aa for rec1 in bb if rec['QMGR'] == rec1['QMGR']]
            query_result = pd.DataFrame(cc).drop_duplicates()
            # breakpoint()
            query_result.sort_values(by=['QMGR'], inplace=True)
            shuffled_columns = query_result[columns_to_shuffle]
            query_result = pd.concat([shuffled_columns, query_result.drop(columns=columns_to_shuffle)], axis=1)
            # breakpoint()
            query_result = query_result.to_dict('records')
            # breakpoint()

        elif 'source_data_qmgr' in formdata and formdata['source_data_qmgr'] == 'qmgr':
            qmgr_data = []
            columns = []
            filter_type = ''
            qmgr_keys = {
                'CHLTYPE': ['svrconn', 'sdr', 'rcvr', 'clussdr', 'clusrcvr'],
                'QTYPE': ['local', 'remote', 'alias']
            }
            if formdata['mq_qmgr_object'] == 'channel':
                # columns_to_shuffle = ['QMGR', 'HOST', 'CHLTYPE', 'CHANNEL', 'service.NAME', 'contact.DATA']
                qmgr_data, errorneous_records = self.get_channels(all_qmgrs)
                op = [formdata[rec] for rec in qmgr_keys['CHLTYPE'] if rec in formdata and formdata[rec]]
                op = list(set(self.split_user_input(','.join(op))))
                chl_df = pd.DataFrame(qmgr_data, columns=['QMGR', 'CHLTYPE', 'CHANNEL'] + op)
                query_result = chl_df.to_dict('records')
            elif formdata['mq_qmgr_object'] == 'queue':
                qmgr_data, errorneous_records = self.get_queues(all_qmgrs)
                op = [formdata[rec] for rec in qmgr_keys['QTYPE'] if rec in formdata and formdata[rec]]
                op = list(set(self.split_user_input(','.join(op))))
                queue_df = pd.DataFrame(qmgr_data, columns=['QMGR', 'QTYPE', 'QUEUE'] + op)
                query_result = queue_df.to_dict('records')

            # breakpoint()


            # if formdata['mq_qmgr_object'] == 'channel':
            #     # breakpoint()
            #     filter_type = 'CHLTYPE'
            #     columns = ['QMGR', 'CHLTYPE', 'CHANNEL']
            #     qmgr_data, errorneous_records = self.get_channels(all_qmgrs)
            # elif formdata['mq_qmgr_object'] == 'queue':
            #     filter_type = 'QTYPE'
            #     columns = ['QMGR', 'QTYPE', 'QUEUE']
            #     qmgr_data, errorneous_records = self.get_queues(all_qmgrs)
            # type_info = {rec: self.split_user_input(formdata[rec]) for rec in qmgr_keys[filter_type]}
            # qmgr_data = [res for res in qmgr_data if isinstance(res, dict)] # not required
            # # breakpoint()
            # query_result = self.mqsearch_channel_type_dynamic_query(formdata=formdata, query_data=qmgr_data, filter_type=filter_type,
            #                                                         type_info=type_info, columns=columns)


        elif 'source_data_inventory' in formdata and formdata['source_data_inventory'] == 'inventory':
            # errorneous_records = {"errors": ""}
            qmgr_data = []
            columns = []
            filter_type = ''
            qmgr_keys = {
                # 'INVENTORY': ['inventory'],
                'INVENTORY': ['inv_acc_qmgr', 'inv_acc_host', 'inv_acc_service', 'inv_acc_contact'],
            }
            filter_type = 'INVENTORY'
            # columns = ['qmgr.NAME', 'host.NAME']
            # columns = ['qmgr.NAME', 'host.NAME', formdata['inv_acc_qmgr'], formdata['inv_acc_host'], formdata['inv_acc_service'], formdata['inv_acc_contact']]
            columns = ['qmgr.NAME', 'host.NAME']
            # columns = ['qmgr.NAME', 'host.NAME', 'HOST_STATUS']
            # inventory_data = self.get_inventories_qmgr_data(all_qmgrs, formdata)
            # if formdata['query_by'] == 'qry_qmgr':
            #     inventory_data, errorneous_records = self.get_inventories_qmgr_data(all_qmgrs)
            # else:
            #     inventory_data, errorneous_records = self.get_inventories_host_data(all_qmgrs)

            if formdata['query_by'] == 'qry_qmgr':
                # https://umc-inventory.prod.gaiacloud.jpmchase.net/umcfg_api/v1/service?limit=-1&qmgr.NAME=APPA600&related_object=host,qmgr
                # APTA600 not working --> testing
                inventory_service_data = self.get_inventory_service_data(all_qmgrs, flag='qmgr')
                # breakpoint()
                # inventory_service_df = pd.DataFrame(inventory_service_data)
                inventory_service_df = pd.DataFrame(inventory_service_data)
                inv_data = {}
                # if inventory_service_data:
                if len(inventory_service_df):
                    columns.extend(['service.NAME', 'contact.DATA'])
                    inv_data = set(inventory_service_df['qmgr.NAME'].to_list())
                    inventory_service_df = pd.DataFrame(inventory_service_data)
                    inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
                        .agg({'contact.DATA': ','.join,
                              **{key: 'first' for key in inventory_service_df.keys()
                                 if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                                'contact.DATA']}}).reset_index()
                    # inventory_service_df = inventory_service_df.str.split(',').apply(lambda x : ','.join(set(x)))
                    inventory_service_data = inventory_service_df.to_dict('records')
                # inv_data = {inv_service_data['qmgr.NAME'] for inv_service_data in inventory_service_data}
                # breakpoint()

                is_all_qmgrs = [qmgr for qmgr in all_qmgrs if qmgr not in inv_data]
                if is_all_qmgrs:
                    inventory_qmgr_data, errorneous_records = self.get_inventories_qmgr_data(is_all_qmgrs)
                else:
                    inventory_qmgr_data, errorneous_records = [], []
                # breakpoint()
                inventory_data = inventory_service_data + inventory_qmgr_data
                # breakpoint()


                # inventory_contact_data = self.get_inventory_contact_data(all_qmgrs, flag='qmgr')
                # breakpoint()
                # if inventory_contact_data:
                #     for rec in inventory_data:
                #         rec['contact.DATA'] = ';'.join((list(set([j['contact.DATA'] for j in inventory_contact_data if rec['qmgr.NAME'] == j['qmgr.NAME']]))))
                # breakpoint()

            elif formdata['query_by'] == 'qry_host':
                # https://umc-inventory.prod.gaiacloud.jpmchase.net/umcfg_api/v1/service?limit=-1&host.NAME=iaasa00020143&related_object=host,qmgr
                # specific host se sahi data aa raha h
                # breakpoint()
                inventory_service_data = self.get_inventory_service_data(all_qmgrs, flag='host')

                # inventory_service_df = pd.DataFrame(inventory_service_data, columns=)
                inventory_service_df = pd.DataFrame(inventory_service_data)
                inv_data = {}
                if len(inventory_service_df):
                    columns.extend(['service.NAME', 'contact.DATA'])
                    inv_data = set(inventory_service_df['host.NAME'].to_list())
                    # inventory_service_df = pd.DataFrame(inventory_service_data)
                    inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
                        .agg({'contact.DATA': ','.join,
                              **{key: 'first' for key in inventory_service_df.keys()
                                 if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                                'contact.DATA']}}).reset_index()
                    # inventory_service_df = inventory_service_df.str.split(',').apply(lambda x : ','.join(set(x)))
                    inventory_service_data = inventory_service_df.to_dict('records')


                # inv_data = {inv_service_data['host.NAME'] for inv_service_data in inventory_service_data}
                is_all_qmgrs = [qmgr for qmgr in all_qmgrs if qmgr not in inv_data]

                if is_all_qmgrs:
                    inventory_qmgr_data, errorneous_records = self.get_inventories_host_data(is_all_qmgrs)
                else:
                    inventory_qmgr_data, errorneous_records = [], []
                # breakpoint()
                inventory_data = inventory_service_data + inventory_qmgr_data

                # inventory_contact_data = self.get_inventory_contact_data(all_qmgrs, flag='host')
                # if inventory_contact_data:
                #     for rec in inventory_data:
                #         rec['contact.DATA'] = ';'.join((list(set([j['contact.DATA'] for j in inventory_contact_data if rec['qmgr.NAME'] == j['qmgr.NAME']]))))

                # breakpoint()
            elif formdata['query_by'] == 'qry_service':
                # https://umc-inventory.prod.gaiacloud.jpmchase.net/umcfg_api/v1/service?limit=-1&service.NAME=CIBBTOFC&related_object=host,qmgr
                inventory_data, errorneous_records = self.get_inventory_service_data(all_qmgrs, flag='service'), []
                # breakpoint()
                # inventory_service_df = pd.DataFrame(inventory_data)
                inventory_service_df = pd.DataFrame(inventory_data)
                # inv_data = {}
                if len(inventory_service_df):
                    columns.extend(['service.NAME', 'contact.DATA'])
                    # inv_data = set(inventory_service_df['host.NAME'].to_list())
                    # inventory_service_df = pd.DataFrame(inventory_service_data)
                    inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
                        .agg({'contact.DATA': ','.join,
                              **{key: 'first' for key in inventory_service_df.keys()
                                 if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                                'contact.DATA']}}).reset_index()
                    # inventory_service_df = inventory_service_df.str.split(',').apply(lambda x : ','.join(set(x)))
                    inventory_data = inventory_service_df.to_dict('records')




                # inventory_contact_data = self.get_inventory_contact_data(all_qmgrs, flag='service')
                # if inventory_contact_data:
                #     for rec in inventory_data:
                #         rec['contact.DATA'] = ';'.join((list(set([j['contact.DATA'] for j in inventory_contact_data if rec['qmgr.NAME'] == j['qmgr.NAME']]))))
                # breakpoint()

            else:
                # https://umc-inventory.prod.gaiacloud.jpmchase.net/umcfg_api/v1/service?limit=-1&service.SEAL_ID=23174&related_object=host,qmgr
                inventory_data, errorneous_records = self.get_inventory_service_data(all_qmgrs, flag='sealid'), []
                # breakpoint()
                # inventory_service_df = pd.DataFrame(inventory_data)
                inventory_service_df = pd.DataFrame(inventory_data)
                # inv_data = {}
                if len(inventory_service_df):
                    columns.extend(['service.NAME', 'contact.DATA'])
                    # inv_data = set(inventory_service_df['host.NAME'].to_list())
                    # inventory_service_df = pd.DataFrame(inventory_service_data)
                    inventory_service_df = inventory_service_df.groupby(['qmgr.NAME', 'host.NAME', 'service.NAME'])\
                        .agg({'contact.DATA': ','.join,
                              **{key: 'first' for key in inventory_service_df.keys()
                                 if key not in ['qmgr.NAME', 'host.NAME', 'service.NAME',
                                                'contact.DATA']}}).reset_index()
                    # inventory_service_df = inventory_service_df.str.split(',').apply(lambda x : ','.join(set(x)))
                    inventory_data = inventory_service_df.to_dict('records')

                # inventory_contact_data = self.get_inventory_contact_data(all_qmgrs, flag='sealid')
                # if inventory_contact_data:
                #     for rec in inventory_data:
                #         rec['contact.DATA'] = ';'.join((list(set([j['contact.DATA'] for j in inventory_contact_data if rec['qmgr.NAME'] == j['qmgr.NAME']]))))


            # else:
            #     inventory_data, errorneous_records = self.get_inventories_host_data(all_qmgrs)

            op = [formdata[rec] for rec in qmgr_keys['INVENTORY'] if rec in formdata and formdata[rec]]
            op = list(set(self.split_user_input(','.join(op))))
            inv_df = pd.DataFrame(inventory_data, columns=['qmgr.NAME', 'host.NAME', 'service.NAME', 'contact.DATA'] + op)
            query_result = inv_df.to_dict('records')

            # breakpoint()
            # type_info = {rec: self.split_user_input(formdata[rec]) for rec in qmgr_keys[filter_type]}
            # type_info = qmgr_keys
            # inventory_data = [res for res in inventory_data if isinstance(res, dict)]
            # breakpoint()
            # query_result = self.mqsearch_channel_type_dynamic_query(formdata=formdata, query_data=inventory_data, filter_type=filter_type,
            #                                                         type_info=type_info, columns=columns)
        # breakpoint()
        return query_result, errorneous_records

    def mqsearch_formdata(self, formdata, mail_from, mail_to):
        results = {"message": "No Result Found"}
        # breakpoint()
        if 'source_data_qmgr' in formdata and formdata[
            'source_data_qmgr'] == 'qmgr' and 'source_data_inventory' in formdata and formdata[
            'source_data_inventory'] == 'inventory':
            # breakpoint()
            all_qmgrs, errorneous_resp = self._get_validate_qmgr(formdata)
            if errorneous_resp['errors']:
                return errorneous_resp  # early return
            # breakpoint()
            qmgr_object_resp = self._get_qmgr_object(formdata, all_qmgrs)
            # breakpoint()
            responses = self.make_response(qmgr_object_resp, formdata['mq_qmgr_object'])
            # breakpoint()
            results = self.send_email(formdata, responses, mail_from=mail_from, mail_to=mail_to, mail_cc=None,
                                      subject="MQSearch Data: ")
            # breakpoint()
        elif 'source_data_qmgr' in formdata and formdata['source_data_qmgr'] == 'qmgr':
            all_qmgrs, errorneous_resp = self._get_validate_qmgr(formdata)
            print(len(all_qmgrs), "-----------------------------")
            if errorneous_resp['errors']:
                return errorneous_resp  # early return
            # breakpoint()
            qmgr_object_resp = self._get_qmgr_object(formdata, all_qmgrs)
            # breakpoint()
            responses = self.make_response(qmgr_object_resp, formdata['mq_qmgr_object'])
            # breakpoint()
            results = self.send_email(formdata, responses, mail_from=mail_from, mail_to=mail_to, mail_cc=None,
                                      subject="MQSearch Data: ")
        elif 'source_data_inventory' in formdata and formdata['source_data_inventory'] == 'inventory':
            all_qmgrs, errorneous_resp = self._get_validate_qmgr(formdata)
            print(len(all_qmgrs), "-----------------------------")
            # breakpoint()
            if errorneous_resp['errors']:
                return errorneous_resp  # early return
            # breakpoint()
            qmgr_object_resp = self._get_qmgr_object(formdata, all_qmgrs)
            responses = self.make_response(qmgr_object_resp, formdata['source_data_inventory'])
            # breakpoint()
            results = self.send_email(formdata, responses, mail_from=mail_from, mail_to=mail_to, mail_cc=None,
                                      subject="MQSearch Data: ")
            # breakpoint()
        return results

    def make_response(self, results, filename):
        success = error = ''
        # breakpoint()
        if results[0]:
            success = self.export_to_excel(results[0], filename)
        if results[1]:
            error = results[1]
        return success, error

    def export_to_excel(self, records, filename):
        filename = filename + f'_mqsearch_{self.today}.xlsx'
        # breakpoint()
        df = pd.DataFrame(records)
        df.to_excel(filename, index=False)

        # comment for now
        # with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        #     for key, value in records.items():
        # Create some Pandas dataframes from some data.
        # new_df = pd.DataFrame(value)
        # Write each dataframe to a different worksheet.
        # new_df.to_excel(writer, sheet_name=key, index=False)

        complete_path = os.path.join(ROOT_PATH, filename)
        return complete_path

    def send_email(self, formdata, responses, mail_from, mail_to, mail_cc=None, subject=None):
        """ Send Blackout Email
            Args:
                formdata: string or list of dict
                responses: tuple or other data type
                mail_from: string
                mail_to: string
                mail_cc: list or list of string. Defaults to None.
                subject: string. Defaults to "Blackout:".
        """
        # body = error = ""
        error = ""
        files = None
        # breakpoint()
        body = '<strong>INPUT:<strong> <br>'
        body += str({key: value for key, value in formdata.items() if value})
        if isinstance(responses, tuple):
            if responses[0]:
                # body += '<strong>INPUT:<strong> <br>'
                # body += str({key: value for key, value in formdata.items() if value})
                files = responses[0]  # responses[0] is the path
            else:
                body += "<br>No Data Found<br>"
            if responses[1]:
                error = responses[1]
                body += "<br><br> <b>ERRORNEOUS RESPONSES:<b> <br>"
                body += '<br>'.join(str(ele) for ele in error)
        else:
            body = 'Not a tuple'
        _ = self.email.send_email(body=body, mail_from=mail_from, mail_to=[mail_to], mail_cc=mail_cc, subject=subject,
                                  files=files)
        success = [{'success_message': 'Email Sent Successfully.'}]
        if files:
            _ = self._remove_file(files)
        return success, error

    @staticmethod
    def _remove_file(files):
        if isinstance(files, list):
            for file in files:
                os.remove(file)
        elif isinstance(files, str):
            os.remove(files)
        return True

    @staticmethod
    def split_user_input(user_input: str):
        result = user_input.split(',') if user_input else ''
        return result

# obj = MQSearchEngine()
# sm_dic = {'SVRCONN': [{'MAXINST': 10.0, 'MAXINSTC': 10.0, 'MAXMSGL': 32768, 'SCYEXIT': 'umaudit(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'ECDHE_RSA_AES_128_GCM_SHA256', 'SSLPEER': ''}, {'MAXINST': 10.0, 'MAXINSTC': 10.0, 'MAXMSGL': 4000000, 'SCYEXIT': 'umaudit(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'ECDHE_RSA_AES_128_GCM_SHA256', 'SSLPEER': ''}, {'MAXINST': 500.0, 'MAXINSTC': 500.0, 'MAXMSGL': 4194304, 'SCYEXIT': 'umaudit(SecExit)', 'SSLCAUTH': 'REQUIRED','SSLCIPH': 'ECDHE_RSA_AES_128_GCM_SHA256', 'SSLPEER': ''}, {'MAXINST': 50.0, 'MAXINSTC': 50.0, 'MAXMSGL': 4194304, 'SCYEXIT': 'embsecex(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'TLS_RSA_WITH_AES_128_CBC_SHA256', 'SSLPEER': ''}, {'MAXINST': 20.0, 'MAXINSTC': 20.0, 'MAXMSGL': 4194304, 'SCYEXIT': 'embsecex(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'TLS_RSA_WITH_AES_256_CBC_SHA256', 'SSLPEER': ''}, {'MAXINST': 500.0, 'MAXINSTC': 500.0, 'MAXMSGL': 4194304, 'SCYEXIT': 'embsecex(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'TLS_RSA_WITH_AES_256_CBC_SHA256', 'SSLPEER': ''}, {'MAXINST': 25.0, 'MAXINSTC': 25.0, 'MAXMSGL': 32768, 'SCYEXIT': 'umaudit(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'ANY_TLS12_OR_HIGHER', 'SSLPEER': ''}, {'MAXINST': 100.0, 'MAXINSTC': 100.0, 'MAXMSGL': 4194304, 'SCYEXIT': 'umaudit(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'ECDHE_RSA_AES_256_GCM_SHA384', 'SSLPEER': ''}, {'MAXINST': 25.0, 'MAXINSTC': 25.0, 'MAXMSGL': 4194304, 'SCYEXIT': 'umaudit(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'ECDHE_RSA_AES_256_GCM_SHA384', 'SSLPEER': ''}, {'MAXINST': 25.0, 'MAXINSTC': 25.0, 'MAXMSGL': 104857600, 'SCYEXIT': 'umaudit(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'ECDHE_RSA_AES_256_GCM_SHA384', 'SSLPEER': ''}, {'MAXINST': 100.0, 'MAXINSTC': 100.0, 'MAXMSGL': 104857600, 'SCYEXIT': 'umaudit(SecExit)', 'SSLCAUTH': 'REQUIRED', 'SSLCIPH': 'ECDHE_RSA_AES_128_GCM_SHA256', 'SSLPEER': ''}]}
# obj.to_pandas(sm_dic)
