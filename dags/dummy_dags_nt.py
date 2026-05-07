from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG(
    dag_id="AFL_MTHLY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("AFL_MTHLY_AGG") as AFL_MTHLY_AGG:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("AFL_MTHLY_CASH_FUND") as AFL_MTHLY_CASH_FUND:
        with TaskGroup("AFL_MTHLY_CASH_FUND_External_Sensors") as AFL_MTHLY_CASH_FUND_External_Sensors:
            EmptyOperator(task_id="close_process_complete_AFL_MTHLY_MKT_VAL")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("AFL_MTHLY_External_Sensors") as AFL_MTHLY_External_Sensors:
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_EGL_AFL_2")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_ETA_AFL_2")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_NFS_AFL_2")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_TRN_AFL_2")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_TRP_AFL_2")
    with TaskGroup("AFL_MTHLY_FCI") as AFL_MTHLY_FCI:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("AFL_MTHLY_PC_PRECHECK_Sensors") as AFL_MTHLY_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("AFL_MTHLY_SYNTHETIC_FLOWS") as AFL_MTHLY_SYNTHETIC_FLOWS:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="AFL_MTHLY_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="AFL_MTHLY_MKT_VAL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("AFL_MTHLY_MKT_VAL_AGG") as AFL_MTHLY_MKT_VAL_AGG:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("AFL_MTHLY_MKT_VAL_External_Sensors") as AFL_MTHLY_MKT_VAL_External_Sensors:
        with TaskGroup("AR_MTHLY_FUND_OFFSET") as AR_MTHLY_FUND_OFFSET:
            EmptyOperator(task_id="rcc_close_process_complete_AR_MTHLY_OFFSET")
        with TaskGroup("SL_MTHLY_EGL_AR_EDL") as SL_MTHLY_EGL_AR_EDL:
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_EGL_AR")
        with TaskGroup("SL_MTHLY_GIO_MKT_VAL_EDL") as SL_MTHLY_GIO_MKT_VAL_EDL:
            EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_4")
        with TaskGroup("SL_MTHLY_GSL_AR_EDL") as SL_MTHLY_GSL_AR_EDL:
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_GSL_AR")
        with TaskGroup("SL_MTHLY_HFS_AR_EDL") as SL_MTHLY_HFS_AR_EDL:
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_HFS_AR")
        with TaskGroup("SL_MTHLY_IVT_AR_EDL") as SL_MTHLY_IVT_AR_EDL:
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_IVT_AR")
        with TaskGroup("SL_MTHLY_NBS_AR_EDL") as SL_MTHLY_NBS_AR_EDL:
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_NBS_AR")
        with TaskGroup("SL_MTHLY_NFS_AR_EDL") as SL_MTHLY_NFS_AR_EDL:
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_NFS_AR_2")
        with TaskGroup("SL_MTHLY_TBR_AR_EDL") as SL_MTHLY_TBR_AR_EDL:
            EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_TBR_AR_2")
        with TaskGroup("SL_MTHLY_VLN_AR_EDL") as SL_MTHLY_VLN_AR_EDL:
            EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_VLN_AR_2")
        with TaskGroup("SL_MTHLY_VRP_AR_EDL") as SL_MTHLY_VRP_AR_EDL:
            EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_VRP_AR_2")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_VGR_AR")
    with TaskGroup("AFL_MTHLY_MKT_VAL_FCI") as AFL_MTHLY_MKT_VAL_FCI:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("AFL_MTHLY_MKT_VAL_MAIN") as AFL_MTHLY_MKT_VAL_MAIN:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("AFL_MTHLY_MKT_VAL_PC_PRECHECK_Sensors") as AFL_MTHLY_MKT_VAL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="AFL_MTHLY_MKT_VAL_rerun_check")
    with TaskGroup("AR_MTHLY_MKT_VAL_MNL_UPLD") as AR_MTHLY_MKT_VAL_MNL_UPLD:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="check_finalization_log")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="defer_to_next_bd")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="populate_asof_date")
    EmptyOperator(task_id="trigger_AFL_MTHLY_MKT_VAL")

with DAG(
    dag_id="AFL_QTRLY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="AFL_YRLY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="AR_DLY_CDS_D_TRST",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="AR_MTHLY_CDS_DH_LCTN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="AR_MTHLY_CDS_D_LGL_AGRMT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="AR_MTHLY_CDS_D_RECRD_IND",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="AR_MTHLY_CDS_D_TRNSCTN_PRCSSNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="AR_MTHLY_OFFSET",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("AR_MTHLY_FUND_OFFSET") as AR_MTHLY_FUND_OFFSET:
        with TaskGroup("AR_MTHLY_FUND_OFFSET_External_Sensors") as AR_MTHLY_FUND_OFFSET_External_Sensors:
            with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG_XREF") as SL_MTHLY_GIO_ACCT_HLDNG_XREF:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            with TaskGroup("SL_MTHLY_GIO_MKT_VAL_EDL") as SL_MTHLY_GIO_MKT_VAL_EDL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_4")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_EGL_AR")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_GSL_AR")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_HFS_AR")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_IVT_AR")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_NBS_AR")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_NFS_AR_2")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_TBR_AR_2")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_VGR_AR")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_VLN_AR_2")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_VRP_AR_2")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("AR_MTHLY_OFFSET_PC_PRECHECK_Sensors") as AR_MTHLY_OFFSET_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_DLY_CLNT_PRF_DOD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_DLY_CLNT_PRF_DOD_BRST_ADL") as BP_DLY_CLNT_PRF_DOD_BRST_ADL:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_DLY_CLNT_PRF_DOD_BRST_EDL") as BP_DLY_CLNT_PRF_DOD_BRST_EDL:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_DLY_CLNT_PRF_DOD_External_Sensors") as BP_DLY_CLNT_PRF_DOD_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="BP_DLY_CLNT_PRF_DOD_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_DLY_CLNT_RM_SUM_RPT_CDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_DLY_FR_ALT_ELIMS_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_DLY_FR_ALT_ELIMS_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_DLY_FR_ALTELIMS_101_FIN_BAL") as BP_DLY_FR_ALTELIMS_101_FIN_BAL:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_DLY_FR_ALTELIMS_101_MLED") as BP_DLY_FR_ALTELIMS_101_MLED:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_DLY_FR_ALTELIMS_101_TR_BAL") as BP_DLY_FR_ALTELIMS_101_TR_BAL:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_DLY_FR_ALT_ELIMS_101_External_Sensors") as BP_DLY_FR_ALT_ELIMS_101_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_DLY_101")
        EmptyOperator(task_id="close_process_complete_PC_DLY_101")
    EmptyOperator(task_id="BP_DLY_FR_ALT_ELIMS_101_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_DLY_FR_ELIMS_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_DLY_FR_ELIMS_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_DLY_OPRTNL_DEP_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_DLY_OPRTNL_DEP_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_DLY_OPRTNL_DEP_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_DLY_RISK_FRE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_DLY_RISK_FRE_External_Sensors") as BP_DLY_RISK_FRE_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_DLY_RSK_D_F_BUILD")
        EmptyOperator(task_id="close_process_complete_PC_DLY_104")
    with TaskGroup("BP_DLY_RISK_FRE_P_R") as BP_DLY_RISK_FRE_P_R:
        with TaskGroup("BP_DLY_RISK_FRE_D_CIMS") as BP_DLY_RISK_FRE_D_CIMS:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_D_DPS") as BP_DLY_RISK_FRE_D_DPS:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_D_GTM") as BP_DLY_RISK_FRE_D_GTM:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_D_IBC") as BP_DLY_RISK_FRE_D_IBC:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_D_LNS") as BP_DLY_RISK_FRE_D_LNS:
            with TaskGroup("NCW_DLY_RISK_FRE_D_LNS_EDL_01") as NCW_DLY_RISK_FRE_D_LNS_EDL_01:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_RISK_FRE_D_LNS_EDL_02") as NCW_DLY_RISK_FRE_D_LNS_EDL_02:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("BP_DLY_RISK_FRE_D_LOC") as BP_DLY_RISK_FRE_D_LOC:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_D_MBO") as BP_DLY_RISK_FRE_D_MBO:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_D_MID") as BP_DLY_RISK_FRE_D_MID:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_D_NBS") as BP_DLY_RISK_FRE_D_NBS:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_D_TBR") as BP_DLY_RISK_FRE_D_TBR:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_F_CIMS") as BP_DLY_RISK_FRE_F_CIMS:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_F_DPS") as BP_DLY_RISK_FRE_F_DPS:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_F_GTM") as BP_DLY_RISK_FRE_F_GTM:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_F_IBC") as BP_DLY_RISK_FRE_F_IBC:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_F_LNS") as BP_DLY_RISK_FRE_F_LNS:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_F_LOC") as BP_DLY_RISK_FRE_F_LOC:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_F_MBO") as BP_DLY_RISK_FRE_F_MBO:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_F_MID") as BP_DLY_RISK_FRE_F_MID:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FRE_F_TBR") as BP_DLY_RISK_FRE_F_TBR:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="BP_DLY_RISK_FRE_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_DLY_RISK_FRE_CIMS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_DLY_RSK_D_F_BUILD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_DLY_RISK_FACILITY_P_R") as BP_DLY_RISK_FACILITY_P_R:
        with TaskGroup("BP_DLY_RISK_CIMS_FACILITY_D") as BP_DLY_RISK_CIMS_FACILITY_D:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_CIMS_FACILITY_F") as BP_DLY_RISK_CIMS_FACILITY_F:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_FACILITY_F") as BP_DLY_RISK_FACILITY_F:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_MID_FACILITY_D") as BP_DLY_RISK_MID_FACILITY_D:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RISK_MID_FACILITY_F") as BP_DLY_RISK_MID_FACILITY_F:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_DLY_RSK_FACILITY") as BP_DLY_RSK_FACILITY:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_DLY_RSK_D_F_BUILD_External_Sensors") as BP_DLY_RSK_D_F_BUILD_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_DLY")
        EmptyOperator(task_id="close_process_complete_PC_DLY_104")
        EmptyOperator(task_id="close_process_complete_SL_DLY_CIMS")
    EmptyOperator(task_id="BP_DLY_RSK_D_F_BUILD_rerun_check")
    with TaskGroup("BP_DLY_RSK_FACILITY") as BP_DLY_RSK_FACILITY:
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="BP_MTHLY_FRE_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_DLY_RSK_D_F_BUILD_CIMS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_ABM_DRV",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_ABM_EXP_TYP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_AXM_FRBAL_PF_AVG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_AXM_FRBAL_AVG") as BP_MTHLY_AXM_FRBAL_AVG:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_AXM_FRBAL_LOAD") as BP_MTHLY_AXM_FRBAL_LOAD:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_AXM_FRBAL_PF") as BP_MTHLY_AXM_FRBAL_PF:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_AXM_FRBAL_PF_AVG_External_Sensors") as BP_MTHLY_AXM_FRBAL_PF_AVG_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="BP_MTHLY_AXM_FRBAL_PF_AVG_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_BALSHEET_ALLOC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_BAL_CAP_ALLOC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_BD19_CAP_RPT_01",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_BD3_CSTNG_DRVR_VAL_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_BD3_PRE_CAPALLOC_RPT_01",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_BD3_PRE_CAPALLOC_RPT_02",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_BD3_PST_CAPALLOC_MRPA_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_BD3_REV_DRVR_VAL_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CAPITAL_DRVR_TRGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CAP_EOP_EXTRACT_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_CAP_EOP_EXTRACT_RPT_External_Sensors") as BP_MTHLY_CAP_EOP_EXTRACT_RPT_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_CSP_CRDT_RSK")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_CSP_RSK")
    with TaskGroup("BP_MTHLY_CAP_EOP_INST_EC_RPT") as BP_MTHLY_CAP_EOP_INST_EC_RPT:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_CAP_GC_ALLOC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_CAP_GC_ALLOC_External_Sensors") as BP_MTHLY_CAP_GC_ALLOC_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_CAP_GC_DRVR")
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_CAP_MAN_INPUT_DRVR")
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_CAP_GC_ALLOC_FIN") as BP_MTHLY_CAP_GC_ALLOC_FIN:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_CAP_GC_ALLOC_TR") as BP_MTHLY_CAP_GC_ALLOC_TR:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="BP_MTHLY_CAP_GC_ALLOC_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_CAP_GC_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CAP_MAN_INPUT_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CA_EL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLIPRF",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLNT_EXP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLNT_EXP_TYP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLNT_FTP_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLNT_FTP_SUM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLNT_PRF_FNL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLNT_PRF_LDGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLNT_PRF_PRLM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLNT_PRF_RPT_TRGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CLNT_VOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_COC_ALLOC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_COSTING_DRVR_TRGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CSP_BASEL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_CSP_BASEL_External_Sensors") as BP_MTHLY_CSP_BASEL_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_CSP_BASEL_SUB_PROC") as BP_MTHLY_CSP_BASEL_SUB_PROC:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_CSTING_PNL_VOL_VAL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CSTNG_BU_RPT_BD1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CSTNG_BU_RPT_FNL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_CSTNG_BU_RPT_PRLM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_DS_EBP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FMZ_CASA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FMZ_RECON_OPER_DEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_FMZ_RECON_OPER_DEP_External_Sensors") as BP_MTHLY_FMZ_RECON_OPER_DEP_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_FMZ_RECON_OPER_DEP_MLED") as BP_MTHLY_FMZ_RECON_OPER_DEP_MLED:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_FMZ_TRSRY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_FMZ_TRSRY_External_Sensors") as BP_MTHLY_FMZ_TRSRY_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_FMZ_TRSRY_TRC") as BP_MTHLY_FMZ_TRSRY_TRC:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_FMZ_TRSRY_TRS") as BP_MTHLY_FMZ_TRSRY_TRS:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="BP_MTHLY_FMZ_TRSRY_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_FR_ALT_ELIMS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_FR_ALTELIMS_FIN_BAL") as BP_MTHLY_FR_ALTELIMS_FIN_BAL:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_FR_ALTELIMS_MLED") as BP_MTHLY_FR_ALTELIMS_MLED:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_FR_ALTELIMS_TR_BAL") as BP_MTHLY_FR_ALTELIMS_TR_BAL:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_FR_ALT_ELIMS_External_Sensors") as BP_MTHLY_FR_ALT_ELIMS_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="BP_MTHLY_FR_ALT_ELIMS_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_FR_CDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FR_DMG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FR_ELIMS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FR_RAM_DMG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FR_RAM_DMG_ERD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FTDI",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_FTDI_DRVR") as BP_MTHLY_FTDI_DRVR:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_FTDI_External_Sensors") as BP_MTHLY_FTDI_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_PF")
        EmptyOperator(task_id="close_process_complete_CP_MTHLY")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_FTP_OP_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FTP_OUTBND_TRGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FTP_STAT_ACCTS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_FX_STAT_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_IMPFALL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_INSTR_OP_NON_DEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_INTR_CMPNY_COL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_INTR_CMPNY_COL_CALC") as BP_MTHLY_INTR_CMPNY_COL_CALC:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_INTR_CMPNY_COL_External_Sensors") as BP_MTHLY_INTR_CMPNY_COL_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_PLDG_AMT")
        EmptyOperator(task_id="close_process_complete_CP_MTHLY")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_INTR_CMPNY_COL_OFSAA_OTH") as BP_MTHLY_INTR_CMPNY_COL_OFSAA_OTH:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="BP_MTHLY_INTR_CMPNY_COL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_LCTN_UPD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_LDGR_PUSH_TRST_FEE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_MAIN_CAP_ALLOC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_MAIN_CAP_ALLOC_External_Sensors") as BP_MTHLY_MAIN_CAP_ALLOC_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_CAP_GC_ALLOC")
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_CAP_GC_DRVR")
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_MAIN_CAP_ALLOC_FIN") as BP_MTHLY_MAIN_CAP_ALLOC_FIN:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_MAIN_CAP_ALLOC_INST_EC") as BP_MTHLY_MAIN_CAP_ALLOC_INST_EC:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_MAIN_CAP_ALLOC_TR") as BP_MTHLY_MAIN_CAP_ALLOC_TR:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="BP_MTHLY_MAIN_CAP_ALLOC_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_MAN_ADJ_ARCN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_MAN_ADJ_ARCN_External_Sensors") as BP_MTHLY_MAN_ADJ_ARCN_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_MAN_ADJ_ARCN_SP") as BP_MTHLY_MAN_ADJ_ARCN_SP:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_MAN_ADJ_BCOS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_MAN_ADJ_BREV",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_MAN_ADJ_BTAX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_MAN_ADJ_BTAX_External_Sensors") as BP_MTHLY_MAN_ADJ_BTAX_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_MAN_ADJ_BTAX_SP") as BP_MTHLY_MAN_ADJ_BTAX_SP:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_MAN_ADJ_FNL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_MAN_ADJ_FNL_External_Sensors") as BP_MTHLY_MAN_ADJ_FNL_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_MAN_ADJ_FNL_SP") as BP_MTHLY_MAN_ADJ_FNL_SP:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_MGMT_ADJ_FNCB",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_MGMT_ADJ_FNCB_External_Sensors") as BP_MTHLY_MGMT_ADJ_FNCB_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("BP_MTHLY_MGMT_ADJ_FNCB_SP") as BP_MTHLY_MGMT_ADJ_FNCB_SP:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_MGMT_ADJ_INCB",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_MGMT_ADJ_PRCB",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_MID_MKT_ALLOC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_MMA_STAT_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_NCW_LOAN_EXTRACT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_NII_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_OPER_CAP_STAT_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_OPER_CAP_STAT_DRVR_External_Sensors") as BP_MTHLY_OPER_CAP_STAT_DRVR_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_IMPFALL")
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="BP_MTHLY_OPER_CAP_STAT_DRVR_rerun_check")
    with TaskGroup("BP_MTHLY_OPER_CAP_STAT_FIN_DRVR") as BP_MTHLY_OPER_CAP_STAT_FIN_DRVR:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_OPER_CAP_STAT_TR_DRVR") as BP_MTHLY_OPER_CAP_STAT_TR_DRVR:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_OPRTNL_DEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_PF",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_PF_MEM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_PLDG_AMT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_PLDG_AMT_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_PLDG_AMT_CALC_RPT") as BP_MTHLY_PLDG_AMT_CALC_RPT:
        EmptyOperator(task_id="BP_MTHLY_PLDG_AMT_CALC_RPT_COPY_target_file")
        EmptyOperator(task_id="delete_airflow_target_file")
        EmptyOperator(task_id="export_component")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_PLDG_AMT_RPT_External_Sensors") as BP_MTHLY_PLDG_AMT_RPT_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_PLDG_AMT")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_POST_OFSAA_TRGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_POST_RECON_OFSAA_TRGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_PRE_RECON_OFSAA_TRGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_REVENUE_DRVR_TRGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_RISK_ADDR_MSTR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_RISK_FACILITY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_RISK_FACILITY_External_Sensors") as BP_MTHLY_RISK_FACILITY_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_205")
    with TaskGroup("BP_MTHLY_RISK_FACILITY_P_R") as BP_MTHLY_RISK_FACILITY_P_R:
        with TaskGroup("BP_MTHLY_RISK_CIMS_FACILITY_D") as BP_MTHLY_RISK_CIMS_FACILITY_D:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_CIMS_FACILITY_F") as BP_MTHLY_RISK_CIMS_FACILITY_F:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_LNS_FACILITY_D") as BP_MTHLY_RISK_LNS_FACILITY_D:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_LNS_FACILITY_F") as BP_MTHLY_RISK_LNS_FACILITY_F:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_MID_FACILITY_D") as BP_MTHLY_RISK_MID_FACILITY_D:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_MID_FACILITY_F") as BP_MTHLY_RISK_MID_FACILITY_F:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_TBR_FACILITY_D") as BP_MTHLY_RISK_TBR_FACILITY_D:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_TBR_FACILITY_F") as BP_MTHLY_RISK_TBR_FACILITY_F:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="BP_MTHLY_RISK_FACILITY_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_RISK_FRE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_RISK_FRE_External_Sensors") as BP_MTHLY_RISK_FRE_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_RISK_FACILITY")
    with TaskGroup("BP_MTHLY_RISK_FRE_P_R") as BP_MTHLY_RISK_FRE_P_R:
        with TaskGroup("BP_MTHLY_RISK_FRE_D_CIMS") as BP_MTHLY_RISK_FRE_D_CIMS:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_D_DPS") as BP_MTHLY_RISK_FRE_D_DPS:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_D_GTM") as BP_MTHLY_RISK_FRE_D_GTM:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_D_IBC") as BP_MTHLY_RISK_FRE_D_IBC:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_D_LNS") as BP_MTHLY_RISK_FRE_D_LNS:
            with TaskGroup("NCW_MTHLY_RISK_FRE_D_LNS_EDL_01") as NCW_MTHLY_RISK_FRE_D_LNS_EDL_01:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_RISK_FRE_D_LNS_EDL_02") as NCW_MTHLY_RISK_FRE_D_LNS_EDL_02:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("BP_MTHLY_RISK_FRE_D_LOC") as BP_MTHLY_RISK_FRE_D_LOC:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_D_MBO") as BP_MTHLY_RISK_FRE_D_MBO:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_D_MID") as BP_MTHLY_RISK_FRE_D_MID:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_D_NBS") as BP_MTHLY_RISK_FRE_D_NBS:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_D_TBR") as BP_MTHLY_RISK_FRE_D_TBR:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_CIMS") as BP_MTHLY_RISK_FRE_F_CIMS:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_DPS") as BP_MTHLY_RISK_FRE_F_DPS:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_GTM") as BP_MTHLY_RISK_FRE_F_GTM:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_IBC") as BP_MTHLY_RISK_FRE_F_IBC:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_LNS") as BP_MTHLY_RISK_FRE_F_LNS:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_LOC") as BP_MTHLY_RISK_FRE_F_LOC:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_MBO") as BP_MTHLY_RISK_FRE_F_MBO:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_MID") as BP_MTHLY_RISK_FRE_F_MID:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_NBS") as BP_MTHLY_RISK_FRE_F_NBS:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("BP_MTHLY_RISK_FRE_F_TBR") as BP_MTHLY_RISK_FRE_F_TBR:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="BP_MTHLY_RISK_FRE_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_RISK_FRE_ORGNTN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD1_EXTRACT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD1_HRCHY_EXTRACT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD3_CLNT_INC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD3_DRVR_VOL_ALL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD3_DRVR_VOL_CD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD3_DRVR_VOL_CNTR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD3_DRVR_VOL_ETL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD3_DRVR_VOL_FTP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD3_DRVR_VOL_RD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD3_DRVR_VOL_SRA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD3_GLTRNSCTN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SAP_OUTBND_BD4_FTPRSLT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SECOND_CAP_ALLOC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_SEC_LEND_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_STAT_DRVR_AMND",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_STAT_LEVERAGE_CAP_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BP_MTHLY_STAT_LEVERAGE_CAP_DRVR_External_Sensors") as BP_MTHLY_STAT_LEVERAGE_CAP_DRVR_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_CAP_GC_DRVR")
        EmptyOperator(task_id="close_process_complete_BP_MTHLY_MAIN_CAP_ALLOC")
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="BP_MTHLY_STAT_LEVERAGE_CAP_DRVR_rerun_check")
    with TaskGroup("BP_MTHLY_STAT_LEVERAGE_CAP_FIN_DRVR") as BP_MTHLY_STAT_LEVERAGE_CAP_FIN_DRVR:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("BP_MTHLY_STAT_LEVERAGE_CAP_TR_DRVR") as BP_MTHLY_STAT_LEVERAGE_CAP_TR_DRVR:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="BP_MTHLY_STAT_SEC_LEND_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_STRM_ITRTN_STRT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_MTHLY_THIRD_CAP_ALLOC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_QTRLY_MGR_BRST",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_BSLN_VOL_PLN_MNL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_BSLN_VOL_PLN_PLNTYPE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_BSLN_VOL_PLN_VOLHDR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_CST_ALLC_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_FINCNTR_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_FINMTRC_INSTR_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_OUTBND_DRVR_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_PLNTAX_PF_LDGR_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_PLNTAX_PF_LDGR_PLN_MIDDAY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_PLNTAX_PF_LDGR_PLN_PM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_SECCNDNTL_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_SQL_DRVR_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BP_YRLY_SUBCSTDY_DRVR_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BSIS_MTHLY_BD_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BSIS_MTHLY_BD_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="BSIS_MTHLY_BD_3",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_DLY_CUST",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_DLY_FDA_DEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_DLY_FDA_DEP_FACT_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_DLY_FGL_JRNL_TRN_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_DLY_GRS_COREP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_DLY_GRS_LG_EXPOS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_DLY_LDGR_BALANCE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_DLY_LDGR_NA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_MTHLY_CLNT_NEXP_TYP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_MTHLY_CRNCY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_MTHLY_CRNCY_IMPCT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_MTHLY_FDA_MR_LDGR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_MTHLY_FDA_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_MTHLY_FGL_PPM_ACCT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_MTHLY_MR_LDGR_NA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CDS_MTHLY_TRIL_BLNC_NA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CD_MTHLY_CLIENT_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CD_MTHLY_FINCNTR_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CD_MTHLY_FINMTRC_INSTRMNT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CD_MTHLY_IRAS_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CD_MTHLY_NTGI_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("CD_MTHLY_NTGI_DRVR_External_Sensors") as CD_MTHLY_NTGI_DRVR_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_SD_MTHLY_BD_2")
        EmptyOperator(task_id="close_process_complete_SD_MTHLY_BD_3")
    with TaskGroup("CD_MTHLY_NTGI_DRVR_TRVOL") as CD_MTHLY_NTGI_DRVR_TRVOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="CD_MTHLY_PACE_BTR_EXTRACT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CD_MTHLY_RM_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("CD_MTHLY_RM_DRVR_External_Sensors") as CD_MTHLY_RM_DRVR_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_DL_MTHLY_INSTRMNT_CHRT_FLDS")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("CD_MTHLY_RM_DRVR_FIN_CALC") as CD_MTHLY_RM_DRVR_FIN_CALC:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CD_MTHLY_RM_DRVR_TR_CALC") as CD_MTHLY_RM_DRVR_TR_CALC:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="CD_MTHLY_RM_DRVR_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="CD_MTHLY_SBK_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("CD_MTHLY_SBK_DRVR_External_Sensors") as CD_MTHLY_SBK_DRVR_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_DL_MTHLY_INSTRMNT_CHRT_FLDS")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("CD_MTHLY_SBK_DRVR_FIN") as CD_MTHLY_SBK_DRVR_FIN:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="CD_MTHLY_SEC_CNDNTL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="CD_MTHLY_RM_DRVR_rerun_check")
    with TaskGroup("CD_MTHLY_SEC_CNDNTL_External_Sensors") as CD_MTHLY_SEC_CNDNTL_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CD_MTHLY_FINMTRC_INSTRMNT")
        EmptyOperator(task_id="close_process_complete_CD_MTHLY_SQL_DRVR")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_RD_MTHLY_REVOL")
    with TaskGroup("CD_MTHLY_SEC_CNDNTL_SRCFINVOL") as CD_MTHLY_SEC_CNDNTL_SRCFINVOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CD_MTHLY_SEC_CNDNTL_SRCTRVOL") as CD_MTHLY_SEC_CNDNTL_SRCTRVOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="CD_MTHLY_SEC_CNDNTL_rerun_check")
    with TaskGroup("PANEL_DATA_VALIDATION") as PANEL_DATA_VALIDATION:
        EmptyOperator(task_id="check_row_count")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="CD_MTHLY_SQL_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CD_MTHLY_SUBCSTDY_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CP_DLY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("CP_DLY_100_External_Sensors") as CP_DLY_100_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BP_DLY_OPRTNL_DEP_100")
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="email_notif_op_complete_SL_DLY_BSIS")
    with TaskGroup("CP_DLY_ELIMS_100") as CP_DLY_ELIMS_100:
        with TaskGroup("CP_DLY_ELIMS_100_FIN_BAL") as CP_DLY_ELIMS_100_FIN_BAL:
            EmptyOperator(task_id="run_ge_sp_01_tests")
            EmptyOperator(task_id="sp_elims_01_load")
        with TaskGroup("CP_DLY_ELIMS_100_MLED_DLY") as CP_DLY_ELIMS_100_MLED_DLY:
            EmptyOperator(task_id="run_ge_sp_03_tests")
            EmptyOperator(task_id="sp_elims_03_load")
        with TaskGroup("CP_DLY_ELIMS_100_TR_BAL") as CP_DLY_ELIMS_100_TR_BAL:
            EmptyOperator(task_id="run_ge_sp_02_tests")
            EmptyOperator(task_id="sp_elims_02_load")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_DLY_FTE_100") as CP_DLY_FTE_100:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_fte_tests")
        EmptyOperator(task_id="sp_fte_01_load")
        EmptyOperator(task_id="sp_fte_02_load")
        EmptyOperator(task_id="sp_fte_03_load")
        EmptyOperator(task_id="sp_fte_04_load")
        EmptyOperator(task_id="sp_fte_05_load")
        EmptyOperator(task_id="sp_fte_06_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_DLY_MTDINC_BAL") as CP_DLY_MTDINC_BAL:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_fin_tests")
        EmptyOperator(task_id="run_ge_tr_tests")
        EmptyOperator(task_id="sp_mtdinc_bal_01_load")
        EmptyOperator(task_id="sp_mtdinc_bal_02_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_DLY_PFAVGCALC_BAL") as CP_DLY_PFAVGCALC_BAL:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_fin_bal_tests")
        EmptyOperator(task_id="run_ge_tr_bal_tests")
        EmptyOperator(task_id="sp_acalc_bal_02_load")
        EmptyOperator(task_id="sp_acalc_bal_04_load")
        EmptyOperator(task_id="sp_pfil_bal_01_load")
        EmptyOperator(task_id="sp_pfil_bal_03_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_DLY_RECON_100") as CP_DLY_RECON_100:
        EmptyOperator(task_id="execute_cleanup_if_rerun")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_tests")
        EmptyOperator(task_id="sp_load_recon_01")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="CP_DLY_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="CP_DLY_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("CP_DLY_101_External_Sensors") as CP_DLY_101_External_Sensors:
        with TaskGroup("SL_DLY_FGL_CP_NA") as SL_DLY_FGL_CP_NA:
            EmptyOperator(task_id="rcc_close_process_complete_SL_DLY_FGL_NA")
        EmptyOperator(task_id="close_process_complete_BP_DLY_OPRTNL_DEP_101")
        EmptyOperator(task_id="close_process_complete_PC_DLY_101")
        EmptyOperator(task_id="close_process_complete_SL_DLY_CIMS")
        EmptyOperator(task_id="close_process_complete_SL_DLY_FGL_NA")
        EmptyOperator(task_id="close_process_complete_SL_DLY_TBR")
    EmptyOperator(task_id="CP_DLY_101_rerun_check")
    with TaskGroup("CP_DLY_ELIMS_101") as CP_DLY_ELIMS_101:
        with TaskGroup("CP_DLY_ELIMS_101_FIN_BAL") as CP_DLY_ELIMS_101_FIN_BAL:
            EmptyOperator(task_id="run_ge_sp_04_tests")
            EmptyOperator(task_id="sp_elims_04_load")
        with TaskGroup("CP_DLY_ELIMS_101_MLED_DLY") as CP_DLY_ELIMS_101_MLED_DLY:
            EmptyOperator(task_id="run_ge_sp_06_tests")
            EmptyOperator(task_id="sp_elims_06_load")
        with TaskGroup("CP_DLY_ELIMS_101_TR_BAL") as CP_DLY_ELIMS_101_TR_BAL:
            EmptyOperator(task_id="run_ge_sp_05_tests")
            EmptyOperator(task_id="sp_elims_05_load")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_DLY_FTE_101") as CP_DLY_FTE_101:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_fte_tests")
        EmptyOperator(task_id="sp_fte_07_load")
        EmptyOperator(task_id="sp_fte_08_load")
        EmptyOperator(task_id="sp_fte_09_load")
        EmptyOperator(task_id="sp_fte_10_load")
        EmptyOperator(task_id="sp_fte_11_load")
        EmptyOperator(task_id="sp_fte_12_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_DLY_MTDINC_BAL_101") as CP_DLY_MTDINC_BAL_101:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_fin_tests")
        EmptyOperator(task_id="run_ge_tr_tests")
        EmptyOperator(task_id="sp_mtdinc_bal_03_load")
        EmptyOperator(task_id="sp_mtdinc_bal_04_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_DLY_PFAVGCALC_BAL_101") as CP_DLY_PFAVGCALC_BAL_101:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_fin_bal_tests")
        EmptyOperator(task_id="run_ge_tr_bal_tests")
        EmptyOperator(task_id="sp_acalc_bal_06_load")
        EmptyOperator(task_id="sp_acalc_bal_08_load")
        EmptyOperator(task_id="sp_pfil_bal_05_load")
        EmptyOperator(task_id="sp_pfil_bal_07_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_DLY_RECON_101") as CP_DLY_RECON_101:
        EmptyOperator(task_id="execute_cleanup_if_rerun")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_tests")
        EmptyOperator(task_id="sp_load_recon_02")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="CP_DLY_PFILL_RT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="CP_MTHLY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BSIS_MTHLY_External_Sensors") as BSIS_MTHLY_External_Sensors:
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_CIMS")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_MBO_MM")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_MID_DEP")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_REP")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_TRS_SWAPS")
        EmptyOperator(task_id="email_notif_op_complete_BSIS_MTHLY_BD_1")
    with TaskGroup("CP_MTHLY_AVGCALC_BAL") as CP_MTHLY_AVGCALC_BAL:
        with TaskGroup("SL_MTHLY_CP_FIN_BAL") as SL_MTHLY_CP_FIN_BAL:
            EmptyOperator(task_id="run_ge_fin_bal_tests")
            EmptyOperator(task_id="sp_load_avg_bal_02")
            EmptyOperator(task_id="sp_load_eop_bal_01")
            EmptyOperator(task_id="sp_load_per_bal_03")
        with TaskGroup("SL_MTHLY_CP_TR_BAL") as SL_MTHLY_CP_TR_BAL:
            EmptyOperator(task_id="run_ge_tr_bal_tests")
            EmptyOperator(task_id="sp_load_avg_bal_05")
            EmptyOperator(task_id="sp_load_eop_bal_04")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_MTHLY_INACTIVE_INSTR") as CP_MTHLY_INACTIVE_INSTR:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="CP_MTHLY_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="CP_MTHLY_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("BSIS_MTHLY_External_Sensors") as BSIS_MTHLY_External_Sensors:
        with TaskGroup("CP_MTHLY_AVGCALC_BAL") as CP_MTHLY_AVGCALC_BAL:
            EmptyOperator(task_id="rcc_close_process_complete_CP_MTHLY")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_FGL_CP")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_HFM")
        EmptyOperator(task_id="email_notif_op_complete_BSIS_MTHLY_BD_2")
        EmptyOperator(task_id="email_notif_op_complete_BSIS_MTHLY_BD_3")
    EmptyOperator(task_id="CP_MTHLY_2_rerun_check")
    with TaskGroup("CP_MTHLY_ELIMS") as CP_MTHLY_ELIMS:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_fin_tests")
        EmptyOperator(task_id="run_ge_sp_meld_tests")
        EmptyOperator(task_id="run_ge_sp_tr_tests")
        EmptyOperator(task_id="sp_elims_01_load")
        EmptyOperator(task_id="sp_elims_02_load")
        EmptyOperator(task_id="sp_elims_03_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_MTHLY_FTE") as CP_MTHLY_FTE:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_fte_tests")
        EmptyOperator(task_id="sp_fte_01_load")
        EmptyOperator(task_id="sp_fte_02_load")
        EmptyOperator(task_id="sp_fte_03_load")
        EmptyOperator(task_id="sp_fte_04_load")
        EmptyOperator(task_id="sp_fte_05_load")
        EmptyOperator(task_id="sp_fte_06_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_MTHLY_FTE_ADJ") as CP_MTHLY_FTE_ADJ:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_fte_adj_load_tests")
        EmptyOperator(task_id="sp_fte_adj_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_MTHLY_HFM_RECON") as CP_MTHLY_HFM_RECON:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_MTHLY_INC_TRST") as CP_MTHLY_INC_TRST:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_MTHLY_RECON") as CP_MTHLY_RECON:
        EmptyOperator(task_id="execute_cleanup_if_rerun")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_recon_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("CP_MTHLY_RECON_RPTS") as CP_MTHLY_RECON_RPTS:
        with TaskGroup("NCW_MTHLY_RECON_GS_01") as NCW_MTHLY_RECON_GS_01:
            EmptyOperator(task_id="run_ge_sp_gs_tests")
            EmptyOperator(task_id="sp_gs_load")
        with TaskGroup("NCW_MTHLY_RECON_ND_01") as NCW_MTHLY_RECON_ND_01:
            EmptyOperator(task_id="run_ge_sp_nd_tests")
            EmptyOperator(task_id="sp_nd_load")
        with TaskGroup("NCW_MTHLY_RECON_NT_01") as NCW_MTHLY_RECON_NT_01:
            EmptyOperator(task_id="run_ge_sp_nt_tests")
            EmptyOperator(task_id="sp_nt_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="CP_MTHLY_PURCHEXP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("CP_MTHLY_PURCHEXP_External_Sensors") as CP_MTHLY_PURCHEXP_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="email_notif_op_complete_CP_MTHLY_2")
    with TaskGroup("CP_MTHLY_PURCH_EXP") as CP_MTHLY_PURCH_EXP:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="DC_DLY_TRIL_BLNC_EMEA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_EBP_INSTRMNT_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_FISTA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("DL_DLY_FISTA_External_Sensors") as DL_DLY_FISTA_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_101")
    with TaskGroup("DL_DLY_TBR_FISTA") as DL_DLY_TBR_FISTA:
        with TaskGroup("NCW_DLY_TBR_FISTA_ADL_01") as NCW_DLY_TBR_FISTA_ADL_01:
            EmptyOperator(task_id="generic_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_generic_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_TBR_FISTA_STG_01") as NCW_DLY_TBR_FISTA_STG_01:
            EmptyOperator(task_id="generic_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_generic_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="DL_DLY_FTP_INSTRMNT_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_FTP_INSTRMNT_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_FTP_OUTBND_LNS_CNTRCTS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_FTP_OUTBND_LNS_PAY_SCHDL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_ATTR_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("DL_DLY_GRS_ATTR_100_External_Sensors") as DL_DLY_GRS_ATTR_100_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_DLY")
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_SL_DLY_GRS_BAL")
    with TaskGroup("DL_DLY_GRS_ATTR_EDL_100") as DL_DLY_GRS_ATTR_EDL_100:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="DL_DLY_GRS_ATTR_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_CPM_ATTR_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("DL_DLY_GRS_CPM_ATTRIBUTES_100") as DL_DLY_GRS_CPM_ATTRIBUTES_100:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("DL_DLY_GRS_CPM_ATTR_100_External_Sensors") as DL_DLY_GRS_CPM_ATTR_100_External_Sensors:
        EmptyOperator(task_id="close_process_complete_DL_DLY_GRS_ATTR_100")
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_SL_DLY_GRS_BAL")
        EmptyOperator(task_id="close_process_complete_SL_DLY_MDS_CPM_ADL")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="DL_DLY_GRS_CPM_ATTR_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_CPM_EXP_DER_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_GSL_EAD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_LANID_LOC_LKP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("DL_DLY_GRS_LANID_LOC_LKP_01") as DL_DLY_GRS_LANID_LOC_LKP_01:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("DL_DLY_GRS_LANID_LOC_LKP_External_Sensors") as DL_DLY_GRS_LANID_LOC_LKP_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_SL_DLY_LANID")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="DL_DLY_GRS_L_CLNT_INSTR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_L_CLNT_INSTR_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_NON_GL_AMT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_NON_GL_AMT_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_NSFR_REGH",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_RELTN_ACCT_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_RELTN_ACCT_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_GRS_ULT_P",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_INSTRMNT_CHRT_CONS_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_INSTRMNT_CHRT_CONS_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_MRL_FCST",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_NTAM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_DLY_OFSAA_LNS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_DMG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("DL_MTHLY_DMG_AR_MKT_CDS") as DL_MTHLY_DMG_AR_MKT_CDS:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("DL_MTHLY_DMG_CUST_CDS") as DL_MTHLY_DMG_CUST_CDS:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("DL_MTHLY_DMG_External_Sensors") as DL_MTHLY_DMG_External_Sensors:
        EmptyOperator(task_id="close_process_complete_AFL_MTHLY_MKT_VAL")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("DL_MTHLY_DMG_TRST_CDS") as DL_MTHLY_DMG_TRST_CDS:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="DL_MTHLY_DMG_rerun_check")
    EmptyOperator(task_id="check_finalization_log")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="defer_to_next_bd")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="populate_asof_date")
    EmptyOperator(task_id="trigger_DL_MTHLY_DMG")

with DAG(
    dag_id="DL_MTHLY_EBP_OUTBOUND_INSTRMNT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_MTCH_CST",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_BLM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_CASA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_CMMTMNT_CNTRCTS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_INVSTMNTS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_INV_PAY_SCHDL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_LDGR_STAT_INSTRMNT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_LNS_CNTRCTS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_LNS_PAY_SCHDL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_OTHSRVCS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBND_TD_CNTRCTS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_FTP_OUTBOUND_INSTRMNT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_GRS_ASST_HLDG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_GRS_ATTR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("DL_MTHLY_GRS_ATTR_EDL") as DL_MTHLY_GRS_ATTR_EDL:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("DL_MTHLY_GRS_ATTR_External_Sensors") as DL_MTHLY_GRS_ATTR_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_GRS_BAL")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="DL_MTHLY_GRS_AUC_AUM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("DL_MTHLY_GRS_AUC_AUM_01") as DL_MTHLY_GRS_AUC_AUM_01:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("DL_MTHLY_GRS_AUC_AUM_External_Sensors") as DL_MTHLY_GRS_AUC_AUM_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_SL_DLY_FGL_CRNCY_EXCHNG")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="DL_MTHLY_GRS_CPM_ATTR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("DL_MTHLY_GRS_CPM_ATTRIBUTES") as DL_MTHLY_GRS_CPM_ATTRIBUTES:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("DL_MTHLY_GRS_CPM_ATTR_External_Sensors") as DL_MTHLY_GRS_CPM_ATTR_External_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_DL_MTHLY_GRS_ATTR")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_SL_DLY_MDS_CPM_ADL")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_GRS_BAL")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="DL_MTHLY_GRS_CPM_ATTR_EMEA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_GRS_GSL_EAD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_GRS_L_CLNT_INSTR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_GRS_NON_GL_AMT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_GRS_RELTN_ACCT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_GRS_SLT_TRNSCTNS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("DL_MTHLY_GRS_SLT_TRNSCTNS_EDL") as DL_MTHLY_GRS_SLT_TRNSCTNS_EDL:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("DL_MTHLY_GRS_SLT_TRNSCTNS_External_Sensors") as DL_MTHLY_GRS_SLT_TRNSCTNS_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="DL_MTHLY_GRS_ULT_P",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_INSTRMNT_CHRT_CONS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_INSTRMNT_CHRT_FLDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_MRL_ACTL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_MRL_FCST",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_PPNR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_MTHLY_STRAT_RSK",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_QTRLY_PPNR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DL_WKLY_QRM_BAL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DS_DLY_OP_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DS_DLY_OP_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="DS_MTHLY_BAL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FPA_MTHLY_ANAPLAN_CCAR_EPR_ACTUALS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FPA_MTHLY_ANAPLAN_DEPARTMENT_PLAN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FPA_MTHLY_ANAPLAN_NFORM_GL_ACCOUNT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FPA_MTHLY_ANAPLAN_PRODUCT_HIERARCHY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FPA_MTHLY_BUS_UNIT_CDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FPA_MTHLY_DEPT_CDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FPA_MTHLY_ERP_ACCT_CDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FPA_MTHLY_GL_ACCT_CDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FPA_MTHLY_PRDCT_CDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("FPA_MTHLY_PRDCT_CDS_PC_PRECHECK_Sensors") as FPA_MTHLY_PRDCT_CDS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_202")
    with TaskGroup("FPA_MTHLY_PRDCT_DH") as FPA_MTHLY_PRDCT_DH:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="FT_DLY_BLOOMBERG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="FT_DLY_Q_HSTRY_BLOOMBERG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_DLY_MUC_SCF_CHG_CCR_FILE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_DLY_MUC_SCF_EUR_CCR_FILE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_DLY_MUC_SCF_GBP_CCR_FILE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_DLY_MUC_SCF_LDN_CCR_FILE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_MTHLY_BQNT_OUT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_MTHLY_LNS_COMMT_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_MTHLY_NCW_VDR_CBL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_MTHLY_NCW_VDR_DUALLY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_MTHLY_NCW_VDR_INSURED",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_MTHLY_NCW_VDR_SWEEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_MTHLY_VDR_AUM_AUC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OB_MTHLY_VDR_SECURITIES",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_ADF_TRIGGER_PIPELINE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_AZURE_LOG_CLEANER",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_BLM_SERVER",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_CDS_PRD_DATA_COPY_SCH_LVL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_COLLIBRA_JOB",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="execute_collibra")
    EmptyOperator(task_id="get_collibra_datasets")

with DAG(
    dag_id="OPS_CONFIG_MIGRATION_DELTA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_CONFIG_MIGRATION_DELTA_V2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="check_delta_migration_status")
    with TaskGroup("delta_migration_using_ff4j") as delta_migration_using_ff4j:
        EmptyOperator(task_id="delta_migration_group")
    EmptyOperator(task_id="process_dag_params")

with DAG(
    dag_id="OPS_CONFIG_MIGRATION_FULL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_CONFIG_MIGRATION_RISK_DELTA_V2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_CONFIG_REVERSE_MIGRATION_FULL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_CONFIG_SLA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_CONTROL_M_JOB",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_DLY_MISSING_DAG_RUN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="detect_skipped_dly_dag_runs")

with DAG(
    dag_id="OPS_DR_CONN_TEST",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="test_dr_connection")

with DAG(
    dag_id="OPS_ENABLE_PROCESS_CONFIG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_FRW_PRD_DATA_COPY_SCH_LVL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_FT_DLY_ARCHIVE_SRC_FILES",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_FT_MTHLY_PURGE_DLY_SRC_FILES",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_FT_MTHLY_UNARCHIVE_SRC_FILES",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_GENERIC_EDL_COMPONENT_RERUN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="comp_load")
    EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="run_ge_test")

with DAG(
    dag_id="OPS_NCW_PRD_DATA_COPY_SCH_LVL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_PROD_CLONE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_SHARE_DATA_REF",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="OPS_SKV_ROTATION",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="branch_on_platform")
    EmptyOperator(task_id="push_sf_secrets_to_ncw_vault")
    EmptyOperator(task_id="skip_update_dockerconfigjson_secrets")
    EmptyOperator(task_id="update_dockerconfigjson_secrets")

with DAG(
    dag_id="OPS_SRC_FILE_LAN_TO_NCW",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="check_files_lan_directory")
    EmptyOperator(task_id="copy_src_files")

with DAG(
    dag_id="OPS_STALE_DAGS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="fetch_stale_dags")
    EmptyOperator(task_id="send_notification")

with DAG(
    dag_id="OPS_UAT1_REFRESH",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_DLY_100",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("PC_DLY_100_External_Sensors") as PC_DLY_100_External_Sensors:
        with TaskGroup("CP_DLY_PFAVGCALC_BAL") as CP_DLY_PFAVGCALC_BAL:
            EmptyOperator(task_id="rcc_close_process_complete_CP_DLY")
        EmptyOperator(task_id="close_process_complete_CP_DLY")
        EmptyOperator(task_id="close_process_complete_PC_DLY_100_V2")
        EmptyOperator(task_id="close_process_complete_SL_DLY_FGL")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="update_period_control_sp")

with DAG(
    dag_id="PC_DLY_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_DLY_102",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_DLY_104",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_DLY_105",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_DLY_106",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_MTHLY_105",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_MTHLY_105_STRT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_MTHLY_200",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_MTHLY_201",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_MTHLY_202",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="update_period_control_sp")

with DAG(
    dag_id="PC_MTHLY_203",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_MTHLY_205",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_MTHLY_206",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="PC_QTRLY_300",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="RD_MTHLY_FDIC_REVDRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="RD_MTHLY_NTSI_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="RD_MTHLY_REVOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("RD_MTHLY_REVOL_MLPFALL") as RD_MTHLY_REVOL_MLPFALL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("RD_MTHLY_REVOL_PC_PRECHECK_Sensors") as RD_MTHLY_REVOL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_CP_MTHLY_2")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_SUBC")
    with TaskGroup("RD_MTHLY_REVOL_SRCTRVOL") as RD_MTHLY_REVOL_SRCTRVOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("RD_MTHLY_REVOL_TRBAL") as RD_MTHLY_REVOL_TRBAL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("RD_MTHLY_REVOL_TRFI_LDGR") as RD_MTHLY_REVOL_TRFI_LDGR:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="RD_MTHLY_REVOL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="RD_MTHLY_TONII_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SD_MTHLY",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SD_MTHLY_BD_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SD_MTHLY_BD_3",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_ACH",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_ACH_PC_PRECHECK_Sensors") as SL_DLY_ACH_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    with TaskGroup("SL_DLY_ACH_VOL") as SL_DLY_ACH_VOL:
        EmptyOperator(task_id="SL_DLY_ACH_VOL_copy_source_file")
        EmptyOperator(task_id="SL_DLY_ACH_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_ACH_VOL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_AIP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_AIP_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_AIP_AR_HLDNG") as SL_DLY_AIP_AR_HLDNG:
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_AIP_AR_PC_PRECHECK_Sensors") as SL_DLY_AIP_AR_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_AR_AUC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_BLM_MKT_ISS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_BRWR_RTNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_BSIS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_CCR_CLNDR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_CCR_FX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_CCR_FX_EAD") as SL_DLY_CCR_FX_EAD:
        with TaskGroup("SL_DLY_CCR_FX_EAD_bal_fact_load") as SL_DLY_CCR_FX_EAD_bal_fact_load:
            EmptyOperator(task_id="balance_fact_load")
            EmptyOperator(task_id="run_ge_edl_balance_fact_tests")
        EmptyOperator(task_id="SL_DLY_CCR_FX_EAD_copy_source_file_adls")
        with TaskGroup("SL_DLY_CCR_FX_EAD_instr_fact_load") as SL_DLY_CCR_FX_EAD_instr_fact_load:
            EmptyOperator(task_id="instr_fact_load")
            EmptyOperator(task_id="run_ge_edl_instr_fact_tests")
        EmptyOperator(task_id="SL_DLY_CCR_FX_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_CCR_FX_PC_PRECHECK_Sensors") as SL_DLY_CCR_FX_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    with TaskGroup("SL_DLY_CCR_FX_TRADE") as SL_DLY_CCR_FX_TRADE:
        EmptyOperator(task_id="SL_DLY_CCR_FX_TRADE_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_CCR_FX_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_CCR_FX_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_CFR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_CIMS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="execute_collibra")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        EmptyOperator(task_id="pull_collibra_datasets")
    with TaskGroup("SL_DLY_CIMS_COMMITMENTS") as SL_DLY_CIMS_COMMITMENTS:
        with TaskGroup("SL_DLY_CIMS_COMMITMENTS_bal_fact_load") as SL_DLY_CIMS_COMMITMENTS_bal_fact_load:
            EmptyOperator(task_id="balance_fact_load")
            EmptyOperator(task_id="run_ge_edl_balance_fact_tests")
        EmptyOperator(task_id="SL_DLY_CIMS_COMMITMENTS_copy_source_file")
        EmptyOperator(task_id="SL_DLY_CIMS_COMMITMENTS_copy_source_file_adls")
        with TaskGroup("SL_DLY_CIMS_COMMITMENTS_instr_fact_load") as SL_DLY_CIMS_COMMITMENTS_instr_fact_load:
            EmptyOperator(task_id="instr_fact_load")
            EmptyOperator(task_id="run_ge_edl_instr_fact_tests")
        EmptyOperator(task_id="SL_DLY_CIMS_COMMITMENTS_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_CIMS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_CIMS_PC_PRECHECK_Sensors") as SL_DLY_CIMS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_COL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_DLY_COL_AGRMT") as SL_DLY_COL_AGRMT:
        EmptyOperator(task_id="SL_DLY_COL_AGRMT_copy_source_file")
        EmptyOperator(task_id="SL_DLY_COL_AGRMT_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_COL_AGRMT_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_COL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="remove_second_line_frm_src_file")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_COL_EXPSR") as SL_DLY_COL_EXPSR:
        EmptyOperator(task_id="SL_DLY_COL_EXPSR_copy_source_file")
        EmptyOperator(task_id="SL_DLY_COL_EXPSR_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_COL_EXPSR_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_COL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load_child")
        EmptyOperator(task_id="dim_load_parent")
        EmptyOperator(task_id="fact_load_child")
        EmptyOperator(task_id="fact_load_parent")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="remove_second_line_frm_src_file")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_child_tests")
        EmptyOperator(task_id="run_ge_edl_dim_parent_tests")
        EmptyOperator(task_id="run_ge_edl_fact_child_tests")
        EmptyOperator(task_id="run_ge_edl_fact_parent_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_COL_HLDGS") as SL_DLY_COL_HLDGS:
        EmptyOperator(task_id="SL_DLY_COL_HLDGS_copy_source_file")
        EmptyOperator(task_id="SL_DLY_COL_HLDGS_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_COL_HLDGS_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_COL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="remove_second_line_frm_src_file")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_COL_PC_PRECHECK_Sensors") as SL_DLY_COL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    with TaskGroup("SL_DLY_COL_STTLMNT") as SL_DLY_COL_STTLMNT:
        EmptyOperator(task_id="SL_DLY_COL_STTLMNT_copy_source_file")
        EmptyOperator(task_id="SL_DLY_COL_STTLMNT_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_COL_STTLMNT_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_COL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="remove_second_line_frm_src_file")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_COL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_DPS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("CDS_DLY_VRF_LDGR_BAL_DTL") as CDS_DLY_VRF_LDGR_BAL_DTL:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_sp_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="execute_collibra")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        EmptyOperator(task_id="pull_collibra_datasets")
    with TaskGroup("SL_DLY_DPS_BALANCE") as SL_DLY_DPS_BALANCE:
        EmptyOperator(task_id="SL_DLY_DPS_BALANCE_copy_source_file")
        EmptyOperator(task_id="SL_DLY_DPS_BALANCE_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_DPS_BALANCE_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_DPS_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_DPS_NMD") as SL_DLY_DPS_NMD:
        EmptyOperator(task_id="SL_DLY_DPS_NMD_copy_source_file")
        EmptyOperator(task_id="SL_DLY_DPS_NMD_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_DPS_NMD_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_DPS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_DPS_PC_PRECHECK_Sensors") as SL_DLY_DPS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    with TaskGroup("SL_DLY_DPS_TD") as SL_DLY_DPS_TD:
        EmptyOperator(task_id="SL_DLY_DPS_TD_copy_source_file")
        EmptyOperator(task_id="SL_DLY_DPS_TD_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_DPS_TD_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_DPS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_DPS_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_DPS_CSTNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_DPS_CSTNG_PC_PRECHECK_Sensors") as SL_DLY_DPS_CSTNG_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    with TaskGroup("SL_DLY_DPS_VOL") as SL_DLY_DPS_VOL:
        EmptyOperator(task_id="SL_DLY_DPS_CSTNG_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_DPS_VOL_copy_source_file")
        EmptyOperator(task_id="SL_DLY_DPS_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_DPS_VOL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_DPS_JNTACCT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_DPS_JNT_ACCTS") as SL_DLY_DPS_JNT_ACCTS:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_DS_PEG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_EBP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_EBP_NOSTRO",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_EBP_SCF_EMEA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_EPM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_EXP_DERIV",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_EXP_DERIV_1") as SL_DLY_EXP_DERIV_1:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_EXP_DERIV_External_Sensors") as SL_DLY_EXP_DERIV_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_FAD_ASST",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FAD_ASST_CST",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FAD_VNDR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FGL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_FGL_ACCT_MAP") as SL_DLY_FGL_ACCT_MAP:
        EmptyOperator(task_id="SL_DLY_FGL_ACCT_MAP_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_ACCT_MAP_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_FGL_BANK") as SL_DLY_FGL_BANK:
        EmptyOperator(task_id="SL_DLY_FGL_BANK_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_BANK_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_FGL_BOOK") as SL_DLY_FGL_BOOK:
        EmptyOperator(task_id="SL_DLY_FGL_BOOK_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_BOOK_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_FGL_DFLT_W") as SL_DLY_FGL_DFLT_W:
        EmptyOperator(task_id="SL_DLY_FGL_DFLT_W_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_DFLT_W_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_FGL_PC_PRECHECK_Sensors") as SL_DLY_FGL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    with TaskGroup("SL_DLY_FGL_PRDCT_CLS") as SL_DLY_FGL_PRDCT_CLS:
        EmptyOperator(task_id="SL_DLY_FGL_PRDCT_CLS_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_PRDCT_CLS_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_FGL_PRJCT_ID") as SL_DLY_FGL_PRJCT_ID:
        EmptyOperator(task_id="SL_DLY_FGL_PRJCT_ID_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_PRJCT_ID_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_FGL_RISK_DOM") as SL_DLY_FGL_RISK_DOM:
        EmptyOperator(task_id="SL_DLY_FGL_RISK_DOM_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_RISK_DOM_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_FGL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_FGL_APAC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_DLY_FGL_APAC_PC_PRECHECK_Sensors") as SL_DLY_FGL_APAC_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    with TaskGroup("SL_DLY_FGL_CP_APAC") as SL_DLY_FGL_CP_APAC:
        EmptyOperator(task_id="SL_DLY_FGL_APAC_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_FGL_CP_APAC_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_CP_APAC_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_FGL_CP_APAC_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="feature_toggle_task_fgl_apac_kafka")
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process_file_load")
        EmptyOperator(task_id="rcc_open_process_generic_load")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_FGL_CRNCY_EXCHNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_FGL_CRNCY_EXCHNG") as SL_DLY_FGL_CRNCY_EXCHNG:
        EmptyOperator(task_id="SL_DLY_FGL_CRNCY_EXCHNG_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_CRNCY_EXCHNG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="global_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="run_ge_glbl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_FGL_CRNCY_EXCHNG_PC_PRECHECK_Sensors") as SL_DLY_FGL_CRNCY_EXCHNG_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_FGL_DPRCTN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FGL_DPRCTN_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FGL_EMEA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_DLY_FGL_CP_EMEA") as SL_DLY_FGL_CP_EMEA:
        EmptyOperator(task_id="SL_DLY_FGL_CP_EMEA_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_CP_EMEA_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="feature_toggle_task_fgl_emea_kafka")
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process_file_load")
        EmptyOperator(task_id="rcc_open_process_generic_load")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_FGL_EMEA_PC_PRECHECK_Sensors") as SL_DLY_FGL_EMEA_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_FGL_FA_PPM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FGL_NA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_DLY_FGL_CP_NA") as SL_DLY_FGL_CP_NA:
        EmptyOperator(task_id="SL_DLY_FGL_CP_NA_copy_source_file")
        EmptyOperator(task_id="SL_DLY_FGL_CP_NA_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_FGL_CP_NA_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_FGL_NA_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="feature_toggle_task_fgl_na_kafka")
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process_file_load")
        EmptyOperator(task_id="rcc_open_process_generic_load")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_FGL_NA_PC_PRECHECK_Sensors") as SL_DLY_FGL_NA_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_FGL_NT_JRNL_APAC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FGL_NT_JRNL_EMEA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FGL_NT_JRNL_NA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FGL_NT_LDGR_APAC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_FGL_NT_LDGR_APAC_PC_PRECHECK_Sensors") as SL_DLY_FGL_NT_LDGR_APAC_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    with TaskGroup("SL_DLY_NT_LDGR_APAC") as SL_DLY_NT_LDGR_APAC:
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_FGL_NT_LDGR_EMEA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FGL_NT_LDGR_EOP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FGL_NT_LDGR_NA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FMX_ALLOC_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FMX_DRVR_PLN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_FMZ_MMC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_FMZ_MMC_External_Sensors") as SL_DLY_FMZ_MMC_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    with TaskGroup("SL_DLY_FMZ_MMC_STG") as SL_DLY_FMZ_MMC_STG:
        EmptyOperator(task_id="SL_DLY_FMZ_MMC_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_FMZ_MMC_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_FMZ_PREPAY_OPTN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_FMZ_PREPAY_OPTN_External_Sensors") as SL_DLY_FMZ_PREPAY_OPTN_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    with TaskGroup("SL_DLY_FMZ_PREPAY_OPTN_STG") as SL_DLY_FMZ_PREPAY_OPTN_STG:
        EmptyOperator(task_id="SL_DLY_FMZ_PREPAY_OPTN_STG_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_FMZ_PREPAY_OPTN_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_GEB",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_GEB_REP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_GRS_BAL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_GRS_BAL_101",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_GRS_INSTR_CUSIP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_GSL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_DLY_GSL_EAD_AGG") as SL_DLY_GSL_EAD_AGG:
        EmptyOperator(task_id="SL_DLY_GSL_EAD_AGG_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GSL_EAD_AGG_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_GSL_EAD_AGG_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_GSL_source_file_sensor")
        EmptyOperator(task_id="adl_agg_lmbrd_load")
        EmptyOperator(task_id="adl_agg_load")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_agg_lmbrd_tests")
        EmptyOperator(task_id="run_ge_adl_agg_tests")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GSL_PC_PRECHECK_Sensors") as SL_DLY_GSL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    with TaskGroup("SL_DLY_GSL_PSTNS") as SL_DLY_GSL_PSTNS:
        EmptyOperator(task_id="SL_DLY_CCR_FX_EAD_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_GSL_PSTNS_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GSL_PSTNS_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_GSL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GSL_QFC_LEND") as SL_DLY_GSL_QFC_LEND:
        EmptyOperator(task_id="SL_DLY_GSL_QFC_LEND_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GSL_QFC_LEND_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_GSL_QFC_LEND_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_GSL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GSL_QFC_REPO") as SL_DLY_GSL_QFC_REPO:
        EmptyOperator(task_id="SL_DLY_GSL_QFC_REPO_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GSL_QFC_REPO_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_GSL_QFC_REPO_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_GSL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_GSL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_GSL_EAD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_GSL_SEC_LEND",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_GSL_SEC") as SL_DLY_GSL_SEC:
        EmptyOperator(task_id="SL_DLY_GSL_SEC_LEND_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_GSL_SEC_copy_source_file_adls")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GSL_SEC_LEND_External_Sensors") as SL_DLY_GSL_SEC_LEND_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_GTM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_GTM_BLM_SEC") as SL_DLY_GTM_BLM_SEC:
        EmptyOperator(task_id="SL_DLY_GTM_BLM_SEC_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_BLM_SEC_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_INSTR") as SL_DLY_GTM_INSTR:
        with TaskGroup("Collibra") as Collibra:
            with TaskGroup("CollibraDQ") as CollibraDQ:
                EmptyOperator(task_id="execute_collibra")
                EmptyOperator(task_id="pull_collibra_datasets")
            with TaskGroup("Control_M") as Control_M:
                EmptyOperator(task_id="execute_control_M")
                EmptyOperator(task_id="pull_control_M")
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("SL_DLY_GTM_BALANCE") as SL_DLY_GTM_BALANCE:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_DLY_GTM_INSTR_ADL") as SL_DLY_GTM_INSTR_ADL:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_DLY_GTM_INSTR_EDL") as SL_DLY_GTM_INSTR_EDL:
            with TaskGroup("NCW_DLY_GTM_INSTR_EDL_01") as NCW_DLY_GTM_INSTR_EDL_01:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_GTM_INSTR_EDL_02") as NCW_DLY_GTM_INSTR_EDL_02:
                EmptyOperator(task_id="fact_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_edl_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="check_edl_task_status")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("SL_DLY_GTM_INSTR_STG") as SL_DLY_GTM_INSTR_STG:
            with TaskGroup("NCW_DLY_GTM_BALANCE_STG_01") as NCW_DLY_GTM_BALANCE_STG_01:
                EmptyOperator(task_id="SL_DLY_GTM_BALANCE_copy_source_file")
                EmptyOperator(task_id="SL_DLY_GTM_BALANCE_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_GTM_BALANCE_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_GTM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_GTM_INSTR_STG_01") as NCW_DLY_GTM_INSTR_STG_01:
                EmptyOperator(task_id="SL_DLY_GTM_INSTR_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_GTM_INSTR_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_GTM_INSTR_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_GTM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_GTM_INSTR_STG_03") as NCW_DLY_GTM_INSTR_STG_03:
                EmptyOperator(task_id="SL_DLY_GTM_INSTR_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_GTM_INSTR_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_GTM_INSTR_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_GTM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_GTM_INSTR_STG_05") as NCW_DLY_GTM_INSTR_STG_05:
                EmptyOperator(task_id="SL_DLY_GTM_INSTR_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_GTM_INSTR_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_GTM_INSTR_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_GTM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("data_gov_control_m_jobs") as data_gov_control_m_jobs:
            EmptyOperator(task_id="execute_ctm_job")
    with TaskGroup("SL_DLY_GTM_PC_PRECHECK_Sensors") as SL_DLY_GTM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_SL_DLY_FGL_CRNCY_EXCHNG")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    with TaskGroup("SL_DLY_GTM_RTNG") as SL_DLY_GTM_RTNG:
        EmptyOperator(task_id="SL_DLY_GTM_RTNG_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_RTNG_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_GTM_RTNG_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_GTM_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_GTM_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_GTM_FED_FUND",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_GTM_PLDG_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_GTM_SMT_RT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_GTM_TNTHOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_GTM_BASIS_SWP") as SL_DLY_GTM_BASIS_SWP:
        EmptyOperator(task_id="SL_DLY_GTM_BASIS_SWP_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_BASIS_SWP_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_BRMDN_SWPTN") as SL_DLY_GTM_BRMDN_SWPTN:
        EmptyOperator(task_id="SL_DLY_GTM_BRMDN_SWPTN_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_BRMDN_SWPTN_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_BSC") as SL_DLY_GTM_BSC:
        EmptyOperator(task_id="SL_DLY_GTM_BSC_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_BSC_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_BSIS_COLLAR") as SL_DLY_GTM_BSIS_COLLAR:
        EmptyOperator(task_id="SL_DLY_GTM_BSIS_COLLAR_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_BSIS_COLLAR_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_CAP_FLOOR") as SL_DLY_GTM_CAP_FLOOR:
        EmptyOperator(task_id="SL_DLY_GTM_CAP_FLOOR_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_CAP_FLOOR_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_ERPN_SWPTN") as SL_DLY_GTM_ERPN_SWPTN:
        EmptyOperator(task_id="SL_DLY_GTM_ERPN_SWPTN_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_ERPN_SWPTN_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_FRA") as SL_DLY_GTM_FRA:
        EmptyOperator(task_id="SL_DLY_GTM_FRA_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_FRA_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_MTM") as SL_DLY_GTM_MTM:
        EmptyOperator(task_id="SL_DLY_GTM_MTM_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_MTM_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_PLDG") as SL_DLY_GTM_PLDG:
        EmptyOperator(task_id="SL_DLY_GTM_PLDG_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_PLDG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_PLN_VNLLA_SWP") as SL_DLY_GTM_PLN_VNLLA_SWP:
        EmptyOperator(task_id="SL_DLY_GTM_PLN_VNLLA_SWP_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_PLN_VNLLA_SWP_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_REPO") as SL_DLY_GTM_REPO:
        with TaskGroup("NCW_DLY_GTM_REPO_FNC_STG_01") as NCW_DLY_GTM_REPO_FNC_STG_01:
            EmptyOperator(task_id="SL_DLY_GTM_REPO_copy_source_file")
            EmptyOperator(task_id="SL_DLY_GTM_REPO_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_GTM_REPO_TRN_STG_01") as NCW_DLY_GTM_REPO_TRN_STG_01:
            EmptyOperator(task_id="SL_DLY_GTM_REPO_copy_source_file")
            EmptyOperator(task_id="SL_DLY_GTM_REPO_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_GTM_REPO_USD_STG_01") as NCW_DLY_GTM_REPO_USD_STG_01:
            EmptyOperator(task_id="SL_DLY_GTM_REPO_copy_source_file")
            EmptyOperator(task_id="SL_DLY_GTM_REPO_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_GTM_SWECF") as SL_DLY_GTM_SWECF:
        EmptyOperator(task_id="SL_DLY_GTM_SWECF_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_SWECF_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_GTM_TNTHOL_PC_PRECHECK_Sensors") as SL_DLY_GTM_TNTHOL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="SL_DLY_GTM_TNTHOL_rerun_check")
    with TaskGroup("SL_DLY_GTM_TRS") as SL_DLY_GTM_TRS:
        EmptyOperator(task_id="SL_DLY_GTM_TRS_copy_source_file")
        EmptyOperator(task_id="SL_DLY_GTM_TRS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_IBC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_IBC_ADL") as SL_DLY_IBC_ADL:
        with TaskGroup("NCW_DLY_IBC_BASEL_ADL_01") as NCW_DLY_IBC_BASEL_ADL_01:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_IBC_BASEL_ADL_02") as NCW_DLY_IBC_BASEL_ADL_02:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="check_adl_task_status")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_IBC_EDL") as SL_DLY_IBC_EDL:
        with TaskGroup("SL_DLY_IBC_BASEL_EDL") as SL_DLY_IBC_BASEL_EDL:
            with TaskGroup("NCW_DLY_IBC_BASEL_EDL_01") as NCW_DLY_IBC_BASEL_EDL_01:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_IBC_BASEL_EDL_02") as NCW_DLY_IBC_BASEL_EDL_02:
                EmptyOperator(task_id="fact_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_edl_tests")
                EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_DLY_IBC_INCEXP_EDL") as SL_DLY_IBC_INCEXP_EDL:
            with TaskGroup("NCW_DLY_IBC_INCEXP_EDL_01") as NCW_DLY_IBC_INCEXP_EDL_01:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_IBC_INCEXP_EDL_02") as NCW_DLY_IBC_INCEXP_EDL_02:
                EmptyOperator(task_id="fact_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_edl_tests")
                EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_IBC_PC_PRECHECK_Sensors") as SL_DLY_IBC_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    with TaskGroup("SL_DLY_IBC_STG") as SL_DLY_IBC_STG:
        with TaskGroup("Collibra") as Collibra:
            with TaskGroup("CollibraDQ") as CollibraDQ:
                EmptyOperator(task_id="execute_collibra")
                EmptyOperator(task_id="pull_collibra_datasets")
            with TaskGroup("Control_M") as Control_M:
                EmptyOperator(task_id="execute_control_M")
                EmptyOperator(task_id="pull_control_M")
            EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("NCW_DLY_IBC_BASEL_STG_01") as NCW_DLY_IBC_BASEL_STG_01:
            EmptyOperator(task_id="SL_DLY_IBC_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_IBC_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_IBC_STG_source_file_sensor")
            EmptyOperator(task_id="SL_DLY_IBC_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_IBC_INCEXP_STG_01") as NCW_DLY_IBC_INCEXP_STG_01:
            EmptyOperator(task_id="SL_DLY_IBC_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_IBC_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_IBC_STG_source_file_sensor")
            EmptyOperator(task_id="SL_DLY_IBC_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="control_m_jobs")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="SL_DLY_IBC_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_IDLE_CSH",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_IDLE_CSH_External_Sensors") as SL_DLY_IDLE_CSH_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_101")
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    with TaskGroup("SL_DLY_TBR_IDLE_CSH") as SL_DLY_TBR_IDLE_CSH:
        EmptyOperator(task_id="SL_DLY_TBR_IDLE_CSH_copy_source_file")
        EmptyOperator(task_id="SL_DLY_TBR_IDLE_CSH_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_INT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_INT_PC_PRECHECK_Sensors") as SL_DLY_INT_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    with TaskGroup("SL_DLY_INT_VOL") as SL_DLY_INT_VOL:
        EmptyOperator(task_id="SL_DLY_INT_VOL_copy_source_file")
        EmptyOperator(task_id="SL_DLY_INT_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_INT_VOL_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_INT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_LANID",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_LANID_PC_PRECHECK_Sensors") as SL_DLY_LANID_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    with TaskGroup("SL_DLY_LANID_VOL") as SL_DLY_LANID_VOL:
        EmptyOperator(task_id="SL_DLY_LANID_VOL_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LANID_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_LANID_VOL_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_LANID_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="feature_toggle_task_4538611_SL_DLY_LANID_VOL_RLS_SP_VB248")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="skip_rls_map_update")
        EmptyOperator(task_id="snowflake_rls_map_update_sp")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_LNS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_DLY_LNS_ACCT") as SL_DLY_LNS_ACCT:
        EmptyOperator(task_id="SL_DLY_LNS_ACCT_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LNS_ACCT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_COMMT_EDL") as SL_DLY_LNS_COMMT_EDL:
        with TaskGroup("NCW_DLY_LNS_COMMT_EDL_01") as NCW_DLY_LNS_COMMT_EDL_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_LNS_COMMT_EDL_02") as NCW_DLY_LNS_COMMT_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_LNS_COMMT_EDL_03") as NCW_DLY_LNS_COMMT_EDL_03:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_LNS_COMMT_NOTE_STG_ADL") as SL_DLY_LNS_COMMT_NOTE_STG_ADL:
        with TaskGroup("SL_DLY_LNS_COMMT") as SL_DLY_LNS_COMMT:
            EmptyOperator(task_id="SL_DLY_LNS_COMMT_copy_source_file")
            EmptyOperator(task_id="SL_DLY_LNS_COMMT_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="run_ge_ultmt_prnt_adl_tests")
            EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="ultmt_prnt_adl_load")
        with TaskGroup("SL_DLY_LNS_NOTE") as SL_DLY_LNS_NOTE:
            EmptyOperator(task_id="SL_DLY_LNS_NOTE_copy_source_file")
            EmptyOperator(task_id="SL_DLY_LNS_NOTE_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_GL_ACBS_EDL") as SL_DLY_LNS_GL_ACBS_EDL:
        with TaskGroup("NCW_DLY_LNS_GL_ACBS_EDL_01") as NCW_DLY_LNS_GL_ACBS_EDL_01:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_LNS_GL_ACBS_EDL_02") as NCW_DLY_LNS_GL_ACBS_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="check_acbsedl_task_status")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_LNS_GL_EDL") as SL_DLY_LNS_GL_EDL:
        with TaskGroup("NCW_DLY_LNS_GL_EDL_01") as NCW_DLY_LNS_GL_EDL_01:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_LNS_GL_EDL_02") as NCW_DLY_LNS_GL_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="check_edl_task_status")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_LNS_GL_STG_LOAD") as SL_DLY_LNS_GL_STG_LOAD:
        with TaskGroup("SL_DLY_LNS_GL_ACBS_STG") as SL_DLY_LNS_GL_ACBS_STG:
            EmptyOperator(task_id="SL_DLY_LNS_GL_ACBS_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_LNS_GL_ACBS_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_DLY_LNS_GL_STG") as SL_DLY_LNS_GL_STG:
            EmptyOperator(task_id="SL_DLY_LNS_GL_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_LNS_GL_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_INTRMDT") as SL_DLY_LNS_INTRMDT:
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_NOTE_EDL") as SL_DLY_LNS_NOTE_EDL:
        with TaskGroup("NCW_DLY_LNS_NOTE_EDL_01") as NCW_DLY_LNS_NOTE_EDL_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_LNS_NOTE_EDL_02") as NCW_DLY_LNS_NOTE_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_LNS_PAY_SCHDL") as SL_DLY_LNS_PAY_SCHDL:
        EmptyOperator(task_id="SL_DLY_LNS_PAY_SCHDL_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LNS_PAY_SCHDL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_PC_PRECHECK_Sensors") as SL_DLY_LNS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="SL_DLY_LNS_rerun_check")
    EmptyOperator(task_id="close_process")
    with TaskGroup("data_gov_control_m_jobs") as data_gov_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_LNS_BCW",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_LNS_BCW_PC_PRECHECK_Sensors") as SL_DLY_LNS_BCW_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="SL_DLY_LNS_BCW_rerun_check")
    with TaskGroup("SL_DLY_LNS_CHRGOF") as SL_DLY_LNS_CHRGOF:
        EmptyOperator(task_id="SL_DLY_LNS_BCW_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_LNS_CHRGOF_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LNS_CHRGOF_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_LNS_CHRGOF_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_COLLAT") as SL_DLY_LNS_COLLAT:
        EmptyOperator(task_id="SL_DLY_LNS_BCW_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_LNS_COLLAT_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LNS_COLLAT_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_LNS_COLLAT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_COLLAT_OBLGTN") as SL_DLY_LNS_COLLAT_OBLGTN:
        EmptyOperator(task_id="SL_DLY_LNS_BCW_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_LNS_COLLAT_OBLGTN_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LNS_COLLAT_OBLGTN_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_LNS_COLLAT_OBLGTN_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_LIAB") as SL_DLY_LNS_LIAB:
        EmptyOperator(task_id="SL_DLY_LNS_BCW_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_LNS_LIAB_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LNS_LIAB_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_LNS_LIAB_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_MCS") as SL_DLY_LNS_MCS:
        EmptyOperator(task_id="SL_DLY_LNS_BCW_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_LNS_MCS_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LNS_MCS_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_LNS_MCS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_PRTCPNT") as SL_DLY_LNS_PRTCPNT:
        EmptyOperator(task_id="SL_DLY_LNS_BCW_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_LNS_PRTCPNT_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LNS_PRTCPNT_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_LNS_PRTCPNT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_LNS_SHRT_MRGN") as SL_DLY_LNS_SHRT_MRGN:
        EmptyOperator(task_id="SL_DLY_LNS_BCW_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_LNS_SHRT_MRGN_copy_source_file")
        EmptyOperator(task_id="SL_DLY_LNS_SHRT_MRGN_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_LNS_SHRT_MRGN_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_LOC_ACCT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="execute_collibra")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        EmptyOperator(task_id="pull_collibra_datasets")
    with TaskGroup("SL_DLY_LOC_ACCT_LOAD") as SL_DLY_LOC_ACCT_LOAD:
        with TaskGroup("SL_DLY_LOC_ACCT_ADL_LOAD") as SL_DLY_LOC_ACCT_ADL_LOAD:
            with TaskGroup("SL_DLY_LOC_ACCT_ADL_01") as SL_DLY_LOC_ACCT_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("SL_DLY_LOC_ACCT_ADL_02") as SL_DLY_LOC_ACCT_ADL_02:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_DLY_LOC_ACCT_EDL") as SL_DLY_LOC_ACCT_EDL:
            with TaskGroup("NCW_DLY_LOC_INSTRMNT_EDL_01") as NCW_DLY_LOC_INSTRMNT_EDL_01:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_INSTRMNT_EDL_02") as NCW_DLY_LOC_INSTRMNT_EDL_02:
                EmptyOperator(task_id="fact_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_edl_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("SL_DLY_LOC_ACCT_LNS_PRECHECK") as SL_DLY_LOC_ACCT_LNS_PRECHECK:
            with TaskGroup("SL_DLY_LNS_COMMT_NOTE_STG_ADL") as SL_DLY_LNS_COMMT_NOTE_STG_ADL:
                with TaskGroup("SL_DLY_LNS_COMMT") as SL_DLY_LNS_COMMT:
                    EmptyOperator(task_id="rcc_close_process_complete_SL_DLY_LNS")
                with TaskGroup("SL_DLY_LNS_NOTE") as SL_DLY_LNS_NOTE:
                    EmptyOperator(task_id="rcc_close_process_complete_SL_DLY_LNS")
        with TaskGroup("SL_DLY_LOC_ACCT_STG_LOAD") as SL_DLY_LOC_ACCT_STG_LOAD:
            with TaskGroup("NCW_DLY_LOC_ACCT_STG_01") as NCW_DLY_LOC_ACCT_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_MIA_ACCT_STG_01") as NCW_DLY_LOC_MIA_ACCT_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_LOC_ACCT_PC_PRECHECK_Sensors") as SL_DLY_LOC_ACCT_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="SL_DLY_LOC_ACCT_rerun_check")
    with TaskGroup("SL_DLY_LOC_DCL_ACCT_LOAD") as SL_DLY_LOC_DCL_ACCT_LOAD:
        with TaskGroup("SL_DLY_LOC_DCL_ACCT_EDL") as SL_DLY_LOC_DCL_ACCT_EDL:
            with TaskGroup("NCW_DLY_LOC_DCL_INSTRMNT_EDL_01") as NCW_DLY_LOC_DCL_INSTRMNT_EDL_01:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_DCL_INSTRMNT_EDL_02") as NCW_DLY_LOC_DCL_INSTRMNT_EDL_02:
                EmptyOperator(task_id="fact_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_edl_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("SL_DLY_LOC_DCL_ACCT_STG_ADL_LOAD") as SL_DLY_LOC_DCL_ACCT_STG_ADL_LOAD:
            with TaskGroup("NCW_DLY_LOC_DCL_ACCT_STG_01") as NCW_DLY_LOC_DCL_ACCT_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_DCL_ACCT_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_DCL_ACCT_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_DCL_ACCT_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_MIA_DCL_ACCT_STG_01") as NCW_DLY_LOC_MIA_DCL_ACCT_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_DCL_ACCT_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_DCL_ACCT_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_DCL_ACCT_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("SL_DLY_LOC_DCL_ACCT_ADL_01") as SL_DLY_LOC_DCL_ACCT_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("SL_DLY_LOC_DCL_ACCT_ADL_02") as SL_DLY_LOC_DCL_ACCT_ADL_02:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_LOC_FEE_LOAD") as SL_DLY_LOC_FEE_LOAD:
        with TaskGroup("SL_DLY_LOC_FEE_ADL_LOAD") as SL_DLY_LOC_FEE_ADL_LOAD:
            with TaskGroup("SL_DLY_LOC_FEE_ADL_01") as SL_DLY_LOC_FEE_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("SL_DLY_LOC_FEE_ADL_02") as SL_DLY_LOC_FEE_ADL_02:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("SL_DLY_LOC_FEE_ADL_03") as SL_DLY_LOC_FEE_ADL_03:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("SL_DLY_LOC_FEE_ADL_04") as SL_DLY_LOC_FEE_ADL_04:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_DLY_LOC_FEE_EDL") as SL_DLY_LOC_FEE_EDL:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_DLY_LOC_FEE_STG_LOAD") as SL_DLY_LOC_FEE_STG_LOAD:
            with TaskGroup("NCW_DLY_LOC_DCL_FEE_STG_01") as NCW_DLY_LOC_DCL_FEE_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_FEE_STG_01") as NCW_DLY_LOC_FEE_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_MIA_DCL_FEE_STG_01") as NCW_DLY_LOC_MIA_DCL_FEE_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_MIA_FEE_STG_01") as NCW_DLY_LOC_MIA_FEE_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_FEE_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_LOC_GL_LOAD") as SL_DLY_LOC_GL_LOAD:
        with TaskGroup("SL_DLY_LOC_GL_ADL_LOAD") as SL_DLY_LOC_GL_ADL_LOAD:
            with TaskGroup("SL_DLY_LOC_GL_ADL_01") as SL_DLY_LOC_GL_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("SL_DLY_LOC_GL_ADL_02") as SL_DLY_LOC_GL_ADL_02:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("SL_DLY_LOC_GL_ADL_03") as SL_DLY_LOC_GL_ADL_03:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("SL_DLY_LOC_GL_ADL_04") as SL_DLY_LOC_GL_ADL_04:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="rcc_close_process")
                EmptyOperator(task_id="rcc_open_process")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_DLY_LOC_GL_EDL") as SL_DLY_LOC_GL_EDL:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_DLY_LOC_GL_STG_LOAD") as SL_DLY_LOC_GL_STG_LOAD:
            with TaskGroup("NCW_DLY_LOC_GL_ACCR_STG_01") as NCW_DLY_LOC_GL_ACCR_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_GL_LIAB_STG_01") as NCW_DLY_LOC_GL_LIAB_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_MIA_GL_ACCR_STG_01") as NCW_DLY_LOC_MIA_GL_ACCR_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_LOC_MIA_GL_LIAB_STG_01") as NCW_DLY_LOC_MIA_GL_LIAB_STG_01:
                EmptyOperator(task_id="SL_DLY_LOC_ACCT_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_LOC_GL_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="check_gl_stg_task_status")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MBO_ACCBAL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_MBO_ACCBAL_AMER") as SL_DLY_MBO_ACCBAL_AMER:
        EmptyOperator(task_id="SL_DLY_MBO_ACCBAL_AMER_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_ACCBAL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
            EmptyOperator(task_id="execute_ctm_job")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_ACCBAL_APAC") as SL_DLY_MBO_ACCBAL_APAC:
        EmptyOperator(task_id="SL_DLY_MBO_ACCBAL_APAC_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_ACCBAL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
            EmptyOperator(task_id="execute_ctm_job")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_ACCBAL_EMEA") as SL_DLY_MBO_ACCBAL_EMEA:
        EmptyOperator(task_id="SL_DLY_MBO_ACCBAL_EMEA_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_ACCBAL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
            EmptyOperator(task_id="execute_ctm_job")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_ACCBAL_PC_PRECHECK_Sensors") as SL_DLY_MBO_ACCBAL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    EmptyOperator(task_id="SL_DLY_MBO_ACCBAL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MBO_CCOLL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_MBO_CCOLL_AMER") as SL_DLY_MBO_CCOLL_AMER:
        EmptyOperator(task_id="SL_DLY_MBO_CCOLL_AMER_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_CCOLL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="external_sensor_for_dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_CCOLL_APAC") as SL_DLY_MBO_CCOLL_APAC:
        EmptyOperator(task_id="SL_DLY_MBO_CCOLL_APAC_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_CCOLL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="external_sensor_for_dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_CCOLL_EMEA") as SL_DLY_MBO_CCOLL_EMEA:
        EmptyOperator(task_id="SL_DLY_MBO_CCOLL_EMEA_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_CCOLL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="external_sensor_for_dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_CCOLL_PC_PRECHECK_Sensors") as SL_DLY_MBO_CCOLL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    with TaskGroup("SL_DLY_MBO_CCOLL_SG") as SL_DLY_MBO_CCOLL_SG:
        EmptyOperator(task_id="SL_DLY_MBO_CCOLL_SG_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_CCOLL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="external_sensor_for_dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_MBO_CCOLL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MBO_FX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_MBO_FX_AMER") as SL_DLY_MBO_FX_AMER:
        EmptyOperator(task_id="SL_DLY_MBO_FX_AMER_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MBO_FX_AMER_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_FX_AMER_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_MBO_FX_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
            EmptyOperator(task_id="execute_ctm_job")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_FX_APAC") as SL_DLY_MBO_FX_APAC:
        EmptyOperator(task_id="SL_DLY_MBO_FX_APAC_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MBO_FX_APAC_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_FX_APAC_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_MBO_FX_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
            EmptyOperator(task_id="execute_ctm_job")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_FX_EMEA") as SL_DLY_MBO_FX_EMEA:
        EmptyOperator(task_id="SL_DLY_MBO_FX_EMEA_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MBO_FX_EMEA_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_FX_EMEA_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_MBO_FX_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
            EmptyOperator(task_id="execute_ctm_job")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_FX_PC_PRECHECK_Sensors") as SL_DLY_MBO_FX_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    EmptyOperator(task_id="SL_DLY_MBO_FX_rerun_check")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MBO_LUX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MBO_MM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_DLY_MBO_MMGLBAL_G") as SL_DLY_MBO_MMGLBAL_G:
        with TaskGroup("SL_DLY_MBO_MMGLBAL") as SL_DLY_MBO_MMGLBAL:
            with TaskGroup("NCW_DLY_MBO_MMGLBAL_AMER_STG_01") as NCW_DLY_MBO_MMGLBAL_AMER_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMGLBAL_APAC_STG_01") as NCW_DLY_MBO_MMGLBAL_APAC_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMGLBAL_EMEA_STG_01") as NCW_DLY_MBO_MMGLBAL_EMEA_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMGLBAL_SG_STG_01") as NCW_DLY_MBO_MMGLBAL_SG_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBAL_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MMGLBA_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="check_adl_fact_task_status")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("SL_DLY_MBO_MMGLBAL_EDL") as SL_DLY_MBO_MMGLBAL_EDL:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="rcc_close_process")
            EmptyOperator(task_id="rcc_open_process")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_MMTRX") as SL_DLY_MBO_MMTRX:
        with TaskGroup("SL_DLY_MBO_MMTRX_ADL") as SL_DLY_MBO_MMTRX_ADL:
            with TaskGroup("NCW_DLY_MBO_MMTRX_AMER_ADL_01") as NCW_DLY_MBO_MMTRX_AMER_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMTRX_APAC_ADL_01") as NCW_DLY_MBO_MMTRX_APAC_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMTRX_EMEA_ADL_01") as NCW_DLY_MBO_MMTRX_EMEA_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMTRX_SG_ADL_01") as NCW_DLY_MBO_MMTRX_SG_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="check_adl_task_status")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("SL_DLY_MBO_MMTRX_EDL") as SL_DLY_MBO_MMTRX_EDL:
            with TaskGroup("NCW_DLY_MBO_MMTRX_EDL_01") as NCW_DLY_MBO_MMTRX_EDL_01:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="external_sensor_for_dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMTRX_EDL_02") as NCW_DLY_MBO_MMTRX_EDL_02:
                EmptyOperator(task_id="fact_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_edl_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="check_edl_task_status")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("SL_DLY_MBO_MMTRX_STG") as SL_DLY_MBO_MMTRX_STG:
            with TaskGroup("NCW_DLY_MBO_MMPD_AMER_STG_01") as NCW_DLY_MBO_MMPD_AMER_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMPD_APAC_STG_01") as NCW_DLY_MBO_MMPD_APAC_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMPD_EMEA_STG_01") as NCW_DLY_MBO_MMPD_EMEA_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="SSL_DLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMTRX_AMER_STG_01") as NCW_DLY_MBO_MMTRX_AMER_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMTRX_APAC_STG_01") as NCW_DLY_MBO_MMTRX_APAC_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMTRX_EMEA_STG_01") as NCW_DLY_MBO_MMTRX_EMEA_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_DLY_MBO_MMTRX_SG_STG_01") as NCW_DLY_MBO_MMTRX_SG_STG_01:
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_DLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="SL_DLY_MBO_MM_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            with TaskGroup("ncw_control_m_jobs_AMER_EMEA") as ncw_control_m_jobs_AMER_EMEA:
                EmptyOperator(task_id="execute_ctm_job")
            with TaskGroup("ncw_control_m_jobs_APAC_SG") as ncw_control_m_jobs_APAC_SG:
                EmptyOperator(task_id="execute_ctm_job")
            EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_MBO_MM_PC_PRECHECK_Sensors") as SL_DLY_MBO_MM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    EmptyOperator(task_id="SL_DLY_MBO_MM_rerun_check")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MBO_NOSTRO_BAL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_MBO_NOSTRO_AMER") as SL_DLY_MBO_NOSTRO_AMER:
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_AMER_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_AMER_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_AMER_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_BAL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_NOSTRO_APAC") as SL_DLY_MBO_NOSTRO_APAC:
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_APAC_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_APAC_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_APAC_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_BAL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_NOSTRO_BAL_External_Sensors") as SL_DLY_MBO_NOSTRO_BAL_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_BAL_rerun_check")
    with TaskGroup("SL_DLY_MBO_NOSTRO_EMEA") as SL_DLY_MBO_NOSTRO_EMEA:
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_BAL_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_EMEA_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_EMEA_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_EMEA_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MBO_NOSTRO_SG") as SL_DLY_MBO_NOSTRO_SG:
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_BAL_source_file_sensor")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_SG_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_SG_copy_source_file_adls")
        EmptyOperator(task_id="SL_DLY_MBO_NOSTRO_SG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MCL_DIM_RSK_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MCL_DIM_RSK_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MCL_FACT_RSK_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MCL_FACT_RSK_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MDS_ADL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_MDS_ADL_01") as SL_DLY_MDS_ADL_01:
        with TaskGroup("NCW_DLY_MDS_ADL_01") as NCW_DLY_MDS_ADL_01:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_02") as NCW_DLY_MDS_ADL_02:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_03") as NCW_DLY_MDS_ADL_03:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_04") as NCW_DLY_MDS_ADL_04:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_05") as NCW_DLY_MDS_ADL_05:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_06") as NCW_DLY_MDS_ADL_06:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_07") as NCW_DLY_MDS_ADL_07:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_08") as NCW_DLY_MDS_ADL_08:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_09") as NCW_DLY_MDS_ADL_09:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_10") as NCW_DLY_MDS_ADL_10:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_11") as NCW_DLY_MDS_ADL_11:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_12") as NCW_DLY_MDS_ADL_12:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_MDS_ADL_02") as SL_DLY_MDS_ADL_02:
        with TaskGroup("NCW_DLY_MDS_ADL_13") as NCW_DLY_MDS_ADL_13:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_14") as NCW_DLY_MDS_ADL_14:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_15") as NCW_DLY_MDS_ADL_15:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_16") as NCW_DLY_MDS_ADL_16:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_ADL_17") as NCW_DLY_MDS_ADL_17:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_MDS_ADL_PC_PRECHECK_Sensors") as SL_DLY_MDS_ADL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="SL_DLY_MDS_ADL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MDS_CPM_ADL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_MDS_CPM_ADL_01") as SL_DLY_MDS_CPM_ADL_01:
        with TaskGroup("NCW_DLY_MDS_CPM_ADL_01") as NCW_DLY_MDS_CPM_ADL_01:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_CPM_ADL_02") as NCW_DLY_MDS_CPM_ADL_02:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_MDS_CPM_ADL_02") as SL_DLY_MDS_CPM_ADL_02:
        with TaskGroup("NCW_DLY_MDS_CPM_ADL_03") as NCW_DLY_MDS_CPM_ADL_03:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_CPM_ADL_04") as NCW_DLY_MDS_CPM_ADL_04:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_CPM_ADL_05") as NCW_DLY_MDS_CPM_ADL_05:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_CPM_ADL_06") as NCW_DLY_MDS_CPM_ADL_06:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_MDS_CPM_ADL_07") as NCW_DLY_MDS_CPM_ADL_07:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_MDS_CPM_ADL_PC_PRECHECK_Sensors") as SL_DLY_MDS_CPM_ADL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="SL_DLY_MDS_CPM_ADL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MDS_CPM_EDL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MDS_EDL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MID_DEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MID_LNS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="execute_collibra")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        EmptyOperator(task_id="pull_collibra_datasets")
    with TaskGroup("SL_DLY_MID_LNS_GBP_COMMT") as SL_DLY_MID_LNS_GBP_COMMT:
        with TaskGroup("SL_DLY_MID_LNS_GBP_COMMT_bal_fact_load") as SL_DLY_MID_LNS_GBP_COMMT_bal_fact_load:
            EmptyOperator(task_id="balance_fact_load")
            EmptyOperator(task_id="run_ge_edl_balance_fact_tests")
        EmptyOperator(task_id="SL_DLY_MID_LNS_GBP_COMMT_copy_source_file")
        with TaskGroup("SL_DLY_MID_LNS_GBP_COMMT_instr_fact_load") as SL_DLY_MID_LNS_GBP_COMMT_instr_fact_load:
            EmptyOperator(task_id="instr_fact_load")
            EmptyOperator(task_id="run_ge_edl_instr_fact_tests")
        EmptyOperator(task_id="SL_DLY_MID_LNS_GBP_COMMT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MID_LNS_NOTE") as SL_DLY_MID_LNS_NOTE:
        EmptyOperator(task_id="SL_DLY_MID_LNS_NOTE_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MID_LNS_NOTE_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MID_LNS_NOTE_BAL") as SL_DLY_MID_LNS_NOTE_BAL:
        EmptyOperator(task_id="SL_DLY_MID_LNS_NOTE_BAL_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MID_LNS_NOTE_BAL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MID_LNS_PC_PRECHECK_Sensors") as SL_DLY_MID_LNS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    with TaskGroup("SL_DLY_MID_LNS_USD_COMMT") as SL_DLY_MID_LNS_USD_COMMT:
        with TaskGroup("SL_DLY_MID_LNS_USD_COMMT_bal_fact_load") as SL_DLY_MID_LNS_USD_COMMT_bal_fact_load:
            EmptyOperator(task_id="balance_fact_load")
            EmptyOperator(task_id="run_ge_edl_balance_fact_tests")
        EmptyOperator(task_id="SL_DLY_MID_LNS_USD_COMMT_copy_source_file")
        with TaskGroup("SL_DLY_MID_LNS_USD_COMMT_instr_fact_load") as SL_DLY_MID_LNS_USD_COMMT_instr_fact_load:
            EmptyOperator(task_id="instr_fact_load")
            EmptyOperator(task_id="run_ge_edl_instr_fact_tests")
        EmptyOperator(task_id="SL_DLY_MID_LNS_USD_COMMT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_MID_LNS_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MID_MUC_DEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MID_MUC_NOSTRO",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MID_NOSTRO",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MITIGNT_COLLAT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MMS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_DLY_MMS_External_Sensors") as SL_DLY_MMS_External_Sensors:
        EmptyOperator(task_id="close_process_complete_SL_DLY_DPS")
    with TaskGroup("SL_DLY_MMS_GL_EDL") as SL_DLY_MMS_GL_EDL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MMS_INSTR_ADL") as SL_DLY_MMS_INSTR_ADL:
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MMS_INSTR_STG") as SL_DLY_MMS_INSTR_STG:
        EmptyOperator(task_id="SL_DLY_MMS_INSTR_STG_copy_source_file")
        EmptyOperator(task_id="SL_DLY_MMS_INSTR_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_MMS_PC_PRECHECK_Sensors") as SL_DLY_MMS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="SL_DLY_MMS_rerun_check")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MRX_MBO_SCF_AMER_REPORT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MRX_MBO_SCF_APAC_REPORT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MRX_MBO_SCF_EMEA_REPORT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MRX_NOSTRO_ACCBAL_AMER",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MRX_NOSTRO_ACCBAL_APAC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MRX_NOSTRO_ACCBAL_EMEA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MSCI",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MSCI_REPORT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_MSCI_REPORT_External_Sensors") as SL_DLY_MSCI_REPORT_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_SL_DLY_MSCI")
    with TaskGroup("SL_DLY_MSCI_SFIGSL_REPORT") as SL_DLY_MSCI_SFIGSL_REPORT:
        EmptyOperator(task_id="SL_DLY_MSCI_SFIGSL_REPORT_COPY_target_file")
        EmptyOperator(task_id="delete_airflow_target_file")
        EmptyOperator(task_id="export_component")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_MSCI_REPO_REPORT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MUC_SCF_CHG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MUC_SCF_EUR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MUC_SCF_GBP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_MUC_SCF_LDN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_NBS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_NBS_ACCT") as SL_DLY_NBS_ACCT:
        EmptyOperator(task_id="SL_DLY_NBS_ACCT_copy_source_file")
        EmptyOperator(task_id="SL_DLY_NBS_ACCT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_NBS_NTSI_MRGLNS") as SL_DLY_NBS_NTSI_MRGLNS:
        EmptyOperator(task_id="SL_DLY_NBS_NTSI_MRGLNS_copy_source_file")
        EmptyOperator(task_id="SL_DLY_NBS_NTSI_MRGLNS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_NBS_PC_PRECHECK_Sensors") as SL_DLY_NBS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="close_process_complete_SL_DLY_FGL_CRNCY_EXCHNG")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    EmptyOperator(task_id="SL_DLY_NBS_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_PSM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_PSM_PC_PRECHECK_Sensors") as SL_DLY_PSM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    with TaskGroup("SL_DLY_PSM_VOL") as SL_DLY_PSM_VOL:
        with TaskGroup("NCW_DLY_PSM_CSTF260_VOL_STG_01") as NCW_DLY_PSM_CSTF260_VOL_STG_01:
            EmptyOperator(task_id="SL_DLY_PSM_VOL_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_PSM_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_PSM_CSTF360_VOL_STG_01") as NCW_DLY_PSM_CSTF360_VOL_STG_01:
            EmptyOperator(task_id="SL_DLY_PSM_VOL_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_PSM_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_PSM_CSTF460_VOL_STG_01") as NCW_DLY_PSM_CSTF460_VOL_STG_01:
            EmptyOperator(task_id="SL_DLY_PSM_VOL_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_PSM_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_PSM_CSTF560_VOL_STG_01") as NCW_DLY_PSM_CSTF560_VOL_STG_01:
            EmptyOperator(task_id="SL_DLY_PSM_VOL_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_PSM_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_PSM_CSTF660_VOL_STG_01") as NCW_DLY_PSM_CSTF660_VOL_STG_01:
            EmptyOperator(task_id="SL_DLY_PSM_VOL_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_PSM_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_PSM_CSTF760_VOL_STG_01") as NCW_DLY_PSM_CSTF760_VOL_STG_01:
            EmptyOperator(task_id="SL_DLY_PSM_VOL_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_PSM_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_PSM_CSTFD60_VOL_STG_01") as NCW_DLY_PSM_CSTFD60_VOL_STG_01:
            EmptyOperator(task_id="SL_DLY_PSM_VOL_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_PSM_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_PSM_F160_VOL_STG_01") as NCW_DLY_PSM_F160_VOL_STG_01:
            EmptyOperator(task_id="SL_DLY_PSM_VOL_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_PSM_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_PSM_F360_VOL_STG_01") as NCW_DLY_PSM_F360_VOL_STG_01:
            EmptyOperator(task_id="SL_DLY_PSM_VOL_copy_source_file_adls")
            EmptyOperator(task_id="SL_DLY_PSM_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_QEB_CASH",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_QEB_FX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_QEB_MM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_SFI",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_SFI_ADL") as SL_DLY_SFI_ADL:
        with TaskGroup("NCW_DLY_SFI_ADL_01") as NCW_DLY_SFI_ADL_01:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_SFI_ADL_02") as NCW_DLY_SFI_ADL_02:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_SFI_ADL_03") as NCW_DLY_SFI_ADL_03:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_SFI_ADL_04") as NCW_DLY_SFI_ADL_04:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="check_adl_task_status")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="execute_ctm_job")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_SFI_GL_EDL") as SL_DLY_SFI_GL_EDL:
        with TaskGroup("NCW_DLY_SFI_GL_EDL_01") as NCW_DLY_SFI_GL_EDL_01:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_SFI_GL_EDL_02") as NCW_DLY_SFI_GL_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="check_gl_edl_task_status")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_SFI_INSTR_EDL") as SL_DLY_SFI_INSTR_EDL:
        with TaskGroup("NCW_DLY_SFI_EDL_INSTR_01") as NCW_DLY_SFI_EDL_INSTR_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_SFI_EDL_INSTR_02") as NCW_DLY_SFI_EDL_INSTR_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_SFI_EDL_INSTR_03") as NCW_DLY_SFI_EDL_INSTR_03:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="check_edl_task_status")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_SFI_PC_PRECHECK_Sensors") as SL_DLY_SFI_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    with TaskGroup("SL_DLY_SFI_STG") as SL_DLY_SFI_STG:
        with TaskGroup("Collibra") as Collibra:
            with TaskGroup("CollibraDQ") as CollibraDQ:
                EmptyOperator(task_id="execute_collibra")
                EmptyOperator(task_id="pull_collibra_datasets")
            with TaskGroup("Control_M") as Control_M:
                EmptyOperator(task_id="execute_control_M")
                EmptyOperator(task_id="pull_control_M")
            EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        with TaskGroup("NCW_DLY_SFI_STG_BAL_01") as NCW_DLY_SFI_STG_BAL_01:
            EmptyOperator(task_id="SL_DLY_SFI_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_SFI_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_SFI_STG_EAD_AGG_01") as NCW_DLY_SFI_STG_EAD_AGG_01:
            EmptyOperator(task_id="SL_DLY_SFI_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_SFI_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_SFI_STG_INSTR_01") as NCW_DLY_SFI_STG_INSTR_01:
            EmptyOperator(task_id="SL_DLY_SFI_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_SFI_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_SFI_STG_INSTR_03") as NCW_DLY_SFI_STG_INSTR_03:
            EmptyOperator(task_id="SL_DLY_SFI_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_SFI_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="SL_DLY_SFI_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_SFI_EAD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_DLY_SFI_INSTRMNT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("OB_DLY_SFI_INSTRMNT_CCR_FILE_01") as OB_DLY_SFI_INSTRMNT_CCR_FILE_01:
        EmptyOperator(task_id="OB_DLY_SFI_INSTRMNT_CCR_FILE_01_COPY_target_file")
        EmptyOperator(task_id="delete_airflow_target_file")
        EmptyOperator(task_id="export_component")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_SFI_INSTRMNT_ADL_CCR") as SL_DLY_SFI_INSTRMNT_ADL_CCR:
        with TaskGroup("SL_DLY_SFI_ADL") as SL_DLY_SFI_ADL:
            EmptyOperator(task_id="close_process_complete_SL_DLY_SFI")
        EmptyOperator(task_id="bypass_external_sensor")
        EmptyOperator(task_id="choose_business_day_or_holiday")
        EmptyOperator(task_id="external_sensor_join")
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_SFI_INSTRMNT_External_Sensors") as SL_DLY_SFI_INSTRMNT_External_Sensors:
        with TaskGroup("SL_DLY_SFI_ADL") as SL_DLY_SFI_ADL:
            EmptyOperator(task_id="rcc_close_process_complete_SL_DLY_SFI")
        EmptyOperator(task_id="close_process_complete_PC_DLY_102")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_TBR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_DLY_TBR_ALLSW") as SL_DLY_TBR_ALLSW:
        EmptyOperator(task_id="SL_DLY_TBR_ALLSW_copy_source_file")
        EmptyOperator(task_id="SL_DLY_TBR_ALLSW_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_TBR_CAYMAN") as SL_DLY_TBR_CAYMAN:
        EmptyOperator(task_id="SL_DLY_TBR_CAYMAN_copy_source_file")
        EmptyOperator(task_id="SL_DLY_TBR_CAYMAN_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_TBR_DEPGL") as SL_DLY_TBR_DEPGL:
        EmptyOperator(task_id="SL_DLY_TBR_DEPGL_copy_source_file")
        EmptyOperator(task_id="SL_DLY_TBR_DEPGL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_TBR_PC_PRECHECK_Sensors") as SL_DLY_TBR_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_101")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    with TaskGroup("SL_DLY_TBR_TRDEM") as SL_DLY_TBR_TRDEM:
        EmptyOperator(task_id="SL_DLY_TBR_TRDEM_copy_source_file")
        EmptyOperator(task_id="SL_DLY_TBR_TRDEM_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_TBR_rerun_check")
    EmptyOperator(task_id="close_process")
    with TaskGroup("data_gov_control_m_jobs") as data_gov_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_TBS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("CollibraDQ") as CollibraDQ:
        EmptyOperator(task_id="execute_CDQ")
        EmptyOperator(task_id="get_CDQ_datasets")
    with TaskGroup("SL_DLY_TBS_PC_PRECHECK_Sensors") as SL_DLY_TBS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
        EmptyOperator(task_id="external_sensor_for_dim_load")
    with TaskGroup("SL_DLY_TBS_SWEEPS") as SL_DLY_TBS_SWEEPS:
        EmptyOperator(task_id="SL_DLY_TBS_SWEEPS_copy_source_file")
        EmptyOperator(task_id="SL_DLY_TBS_SWEEPS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_DLY_TBS_rerun_check")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_TR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("NCW_DLY_TR_ADL_01") as NCW_DLY_TR_ADL_01:
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="stg_i_sp_load")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("NCW_DLY_TR_EDL_01") as NCW_DLY_TR_EDL_01:
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_TR_ASM_STG") as SL_DLY_TR_ASM_STG:
        with TaskGroup("NCW_DLY_TR_ASM_STG_01") as NCW_DLY_TR_ASM_STG_01:
            EmptyOperator(task_id="SL_DLY_TR_ASM_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_TR_ASM_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_TR_ASM_STG_03") as NCW_DLY_TR_ASM_STG_03:
            EmptyOperator(task_id="SL_DLY_TR_ASM_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_TR_ASM_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_TR_ASM_STG_05") as NCW_DLY_TR_ASM_STG_05:
            EmptyOperator(task_id="SL_DLY_TR_ASM_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_TR_ASM_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_DLY_TR_ASM_STG_07") as NCW_DLY_TR_ASM_STG_07:
            EmptyOperator(task_id="SL_DLY_TR_ASM_STG_copy_source_file")
            EmptyOperator(task_id="SL_DLY_TR_ASM_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_DLY_TR_FEP_STG") as SL_DLY_TR_FEP_STG:
        EmptyOperator(task_id="SL_DLY_TR_FEP_STG_copy_source_file")
        EmptyOperator(task_id="SL_DLY_TR_FEP_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_TR_MDS_STG") as SL_DLY_TR_MDS_STG:
        EmptyOperator(task_id="SL_DLY_TR_MDS_STG_copy_source_file")
        EmptyOperator(task_id="SL_DLY_TR_MDS_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_DLY_TR_PC_PRECHECK_Sensors") as SL_DLY_TR_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_DLY_100")
    EmptyOperator(task_id="SL_DLY_TR_onetimeload_check")
    EmptyOperator(task_id="SL_DLY_TR_rerun_check")
    with TaskGroup("SL_OT_TR_TRACCT") as SL_OT_TR_TRACCT:
        EmptyOperator(task_id="SL_OT_TR_TRACCT_copy_source_file")
        EmptyOperator(task_id="SL_OT_TR_TRACCT_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="stg_i_sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    with TaskGroup("data_gov_control_m_jobs") as data_gov_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="mthly_tr_prereq_check")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_DLY_TR_EDL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MANUAL_ADJ_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_AAS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_MTHLY_AAS_GL_EDL") as SL_MTHLY_AAS_GL_EDL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_AAS_INSTR_ADL") as SL_MTHLY_AAS_INSTR_ADL:
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_AAS_INSTR_STG") as SL_MTHLY_AAS_INSTR_STG:
        EmptyOperator(task_id="SL_MTHLY_AAS_INSTR_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_AAS_INSTR_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_AAS_PC_PRECHECK_Sensors") as SL_MTHLY_AAS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_AAS_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_AAS_VOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_AASC_EDL_VOL") as SL_MTHLY_AASC_EDL_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_AAS_EDL_VOL") as SL_MTHLY_AAS_EDL_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_AAS_STG_VOL") as SL_MTHLY_AAS_STG_VOL:
        EmptyOperator(task_id="SL_MTHLY_AAS_STG_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_AAS_STG_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_AAS_VOL_PC_PRECHECK_Sensors") as SL_MTHLY_AAS_VOL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_AAS_VOL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_ACH",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_ACH_PC_PRECHECK_Sensors") as SL_MTHLY_ACH_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_ACH_VOL_EDL") as SL_MTHLY_ACH_VOL_EDL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_AIP_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_AIP_AR_PC_PRECHECK_Sensors") as SL_MTHLY_AIP_AR_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_AIP_AR_STG") as SL_MTHLY_AIP_AR_STG:
        EmptyOperator(task_id="SL_MTHLY_AIP_AR_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_AIP_AR_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_ASM_RSK",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_ASSET",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_ASSET_PC_PRECHECK_Sensors") as SL_MTHLY_ASSET_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_ASSET_VOL") as SL_MTHLY_ASSET_VOL:
        with TaskGroup("NCW_MTHLY_ASSET_VOL_EDL_01") as NCW_MTHLY_ASSET_VOL_EDL_01:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_ASSET_VOL_EDL_02") as NCW_MTHLY_ASSET_VOL_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_ASTUM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_ASTUM_PC_PRECHECK_Sensors") as SL_MTHLY_ASTUM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_ASTUM_VOL") as SL_MTHLY_ASTUM_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_BIC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_BPP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_BQNT_IN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_BQNT_MNL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_BSIS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_BTR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_CA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_CAD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_CIMS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="execute_collibra")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        EmptyOperator(task_id="pull_collibra_datasets")
    with TaskGroup("CollibraDQ") as CollibraDQ:
        EmptyOperator(task_id="execute_collibra")
        EmptyOperator(task_id="pull_collibra_datasets")
    with TaskGroup("SL_MTHLY_CIMS_COMMITMENTS") as SL_MTHLY_CIMS_COMMITMENTS:
        EmptyOperator(task_id="SL_MTHLY_CIMS_COMMITMENTS_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_CIMS_COMMITMENTS_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_CIMS_COMMITMENTS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_CIMS_PC_PRECHECK_Sensors") as SL_MTHLY_CIMS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_CSP_RSK",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_CVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_DDM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_DEPT_PLN_CDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_DPS_CSTNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_DPS_CSTNG_PC_PRECHECK_Sensors") as SL_MTHLY_DPS_CSTNG_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_DPS_VOL") as SL_MTHLY_DPS_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_DS_BD4",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_DUB_CASH",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_EBP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_EBP_TRNSCTN_INTERIM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_EBS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_EGL_AFL_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_EGL_AFL_1_PC_PRECHECK_Sensors") as SL_MTHLY_EGL_AFL_1_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_EGL_AFL_STG") as SL_MTHLY_EGL_AFL_STG:
        EmptyOperator(task_id="SL_MTHLY_EGL_AFL_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_EGL_AFL_STG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_EGL_AFL_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_EGL_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_EGL_AR_EDL") as SL_MTHLY_EGL_AR_EDL:
        with TaskGroup("NCW_MTHLY_EGL_AR_EDL_01") as NCW_MTHLY_EGL_AR_EDL_01:
            with TaskGroup("SL_MTHLY_EGL_AR_EDL_External_Sensors") as SL_MTHLY_EGL_AR_EDL_External_Sensors:
                with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG_XREF") as SL_MTHLY_GIO_ACCT_HLDNG_XREF:
                    EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_EGL_AR_EDL_02") as NCW_MTHLY_EGL_AR_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_EGL_AR_PC_PRECHECK_Sensors") as SL_MTHLY_EGL_AR_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_EGL_AR_STG") as SL_MTHLY_EGL_AR_STG:
        EmptyOperator(task_id="SL_MTHLY_EGL_AR_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_EGL_AR_STG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_EGL_AR_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_EMPOWER",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_EPM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_ETA_AFL_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_ETA_AFL_1_PC_PRECHECK_Sensors") as SL_MTHLY_ETA_AFL_1_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_ETA_AFL_STG") as SL_MTHLY_ETA_AFL_STG:
        EmptyOperator(task_id="SL_MTHLY_ETA_AFL_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_ETA_AFL_STG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_ETA_AFL_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_EXP_DERIV",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FBD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_FBD_PC_PRECHECK_Sensors") as SL_MTHLY_FBD_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_FBD_VOL") as SL_MTHLY_FBD_VOL:
        EmptyOperator(task_id="SL_MTHLY_FBD_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_FBD_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_FBD_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_FDI",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_FDI_ADL") as SL_MTHLY_FDI_ADL:
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_FDI_EDL") as SL_MTHLY_FDI_EDL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_FDI_External_Sensors") as SL_MTHLY_FDI_External_Sensors:
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_MID_DEP")
    with TaskGroup("SL_MTHLY_FDI_PC_PRECHECK_Sensors") as SL_MTHLY_FDI_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_FDI_STG") as SL_MTHLY_FDI_STG:
        with TaskGroup("NCW_MTHLY_FDI_NETTNG_VOL_STG_01") as NCW_MTHLY_FDI_NETTNG_VOL_STG_01:
            EmptyOperator(task_id="SL_MTHLY_FDI_STG_copy_source_file")
            EmptyOperator(task_id="SL_MTHLY_FDI_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_FDI_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_FDI_OVRDRFT_VOL_STG_01") as NCW_MTHLY_FDI_OVRDRFT_VOL_STG_01:
            EmptyOperator(task_id="SL_MTHLY_FDI_STG_copy_source_file")
            EmptyOperator(task_id="SL_MTHLY_FDI_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_FDI_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_FDI_PSTNG_VOL_STG_01") as NCW_MTHLY_FDI_PSTNG_VOL_STG_01:
            EmptyOperator(task_id="SL_MTHLY_FDI_STG_copy_source_file")
            EmptyOperator(task_id="SL_MTHLY_FDI_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_FDI_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="SL_MTHLY_FDI_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_FEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_ASM_ACNS") as SL_MTHLY_ASM_ACNS:
        EmptyOperator(task_id="SL_MTHLY_ASM_ACNS_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_ASM_ACNS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_FEP_ACCT") as SL_MTHLY_FEP_ACCT:
        EmptyOperator(task_id="SL_MTHLY_FEP_ACCT_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_FEP_ACCT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_FEP_ALLOC") as SL_MTHLY_FEP_ALLOC:
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="undefined_product_check")
    with TaskGroup("SL_MTHLY_FEP_FEENPR_ADL") as SL_MTHLY_FEP_FEENPR_ADL:
        EmptyOperator(task_id="SL_MTHLY_FEP_FEENPR_ADL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_FEP_FEENPR_ADL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_FEP_FEENPR_EDL") as SL_MTHLY_FEP_FEENPR_EDL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_FEP_GLBL") as SL_MTHLY_FEP_GLBL:
        EmptyOperator(task_id="SL_MTHLY_FEP_GLBL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_FEP_GLBL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_FEP_PC_PRECHECK_Sensors") as SL_MTHLY_FEP_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_FEP_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="defer_to_next_bd")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_FEPMT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FGL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_FGL_PC_PRECHECK_Sensors") as SL_MTHLY_FGL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_FGL_VOL") as SL_MTHLY_FGL_VOL:
        EmptyOperator(task_id="SL_MTHLY_FGL_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_FGL_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_FGL_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_FGL_CP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
    with TaskGroup("SL_MTHLY_FGL_CP_LGAAP") as SL_MTHLY_FGL_CP_LGAAP:
        EmptyOperator(task_id="SL_MTHLY_FGL_CP_LGAAP_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_FGL_CP_LGAAP_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_FGL_CP_PC_PRECHECK_Sensors") as SL_MTHLY_FGL_CP_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_FGL_CP_USGAAP") as SL_MTHLY_FGL_CP_USGAAP:
        EmptyOperator(task_id="SL_MTHLY_FGL_CP_USGAAP_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_FGL_CP_USGAAP_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_FGL_CP_rerun_check")
    EmptyOperator(task_id="close_process")
    with TaskGroup("control_m_jobs") as control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_FGL_FAD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FGL_FA_DPRCTN_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FGL_JRNL_LN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FGL_VNDR_SPND",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FI_GTM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_FM_PC_PRECHECK_Sensors") as SL_MTHLY_FM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    with TaskGroup("SL_MTHLY_FM_VOL") as SL_MTHLY_FM_VOL:
        EmptyOperator(task_id="SL_MTHLY_FM_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_FM_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_FMS_LUX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FMX_ALLOC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_FMX_ALLOC_External_Sensors") as SL_MTHLY_FMX_ALLOC_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_SL_MTHLY_FMX_DRVR")
    with TaskGroup("SL_MTHLY_FMX_SAP_ALLOC") as SL_MTHLY_FMX_SAP_ALLOC:
        with TaskGroup("NCW_MTHLY_FMX_ALLOC_EDL_01") as NCW_MTHLY_FMX_ALLOC_EDL_01:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_FMX_ALLOC_EDL_02") as NCW_MTHLY_FMX_ALLOC_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_FMX_ALLOC_EDL_03") as NCW_MTHLY_FMX_ALLOC_EDL_03:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_FMX_ALLOC_EDL_04") as NCW_MTHLY_FMX_ALLOC_EDL_04:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_FMX_ALLOC_EDL_05") as NCW_MTHLY_FMX_ALLOC_EDL_05:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_FMX_ALLOC_EDL_06") as NCW_MTHLY_FMX_ALLOC_EDL_06:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_FMX_DRVR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FMZ_BIC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FMZ_IRR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FRP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FRP_ADJMNTS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FRP_CSTNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FTP_GL_ACCT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_FXD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_FXD_PC_PRECHECK_Sensors") as SL_MTHLY_FXD_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_FXD_VOL") as SL_MTHLY_FXD_VOL:
        EmptyOperator(task_id="SL_MTHLY_FXD_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_FXD_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_FXD_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_FXV",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_FXV_PC_PRECHECK_Sensors") as SL_MTHLY_FXV_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_FXV_VOL") as SL_MTHLY_FXV_VOL:
        EmptyOperator(task_id="SL_MTHLY_FXV_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_FXV_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GCM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_GCM_PC_PRECHECK_Sensors") as SL_MTHLY_GCM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_GCM_VOL") as SL_MTHLY_GCM_VOL:
        EmptyOperator(task_id="SL_MTHLY_GCM_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_GCM_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="close_process_complete_SL_MTHLY_LANID")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GCS_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_GDS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_GDS_PC_PRECHECK_Sensors") as SL_MTHLY_GDS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_GDS_TRNSCTNS") as SL_MTHLY_GDS_TRNSCTNS:
        EmptyOperator(task_id="SL_MTHLY_GDS_TRNSCTNS_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_GDS_TRNSCTNS_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_GDS_TRNSCTNS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GDS_CSTNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_GEB",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_GFB",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_GFB_EDL") as SL_MTHLY_GFB_EDL:
        with TaskGroup("NCW_MTHLY_GFB_TRSTFEE_EDL_01") as NCW_MTHLY_GFB_TRSTFEE_EDL_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_GFB_TRSTFEE_EDL_02") as NCW_MTHLY_GFB_TRSTFEE_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_GFB_PC_PRECHECK_Sensors") as SL_MTHLY_GFB_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_GFB_TRSTFEE") as SL_MTHLY_GFB_TRSTFEE:
        EmptyOperator(task_id="SL_MTHLY_GFB_TRSTFEE_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_GFB_TRSTFEE_source_file_sensor")
        with TaskGroup("SL_MTHLY_GFB_UNDFND_PRDCT_CHECK") as SL_MTHLY_GFB_UNDFND_PRDCT_CHECK:
            EmptyOperator(task_id="sp_load")
            EmptyOperator(task_id="undefined_product_check")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_GFB_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="defer_to_next_bd")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GIO_AR_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG") as SL_MTHLY_GIO_ACCT_HLDNG:
        EmptyOperator(task_id="SL_MTHLY_GIO_ACCT_HLDNG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_GIO_ACCT_HLDNG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG_XREF") as SL_MTHLY_GIO_ACCT_HLDNG_XREF:
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_GIO_ACCT_XREF") as SL_MTHLY_GIO_ACCT_XREF:
        EmptyOperator(task_id="SL_MTHLY_GIO_ACCT_XREF_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_GIO_ACCT_XREF_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_GIO_AR_1_PC_PRECHECK_Sensors") as SL_MTHLY_GIO_AR_1_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_GIO_AR_1_rerun_check")
    with TaskGroup("SL_MTHLY_GIO_TRST_ACCT_EXCL") as SL_MTHLY_GIO_TRST_ACCT_EXCL:
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GIO_AR_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_GIO_AR_3",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_GIO_AR_4",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_GIO_AR_4_PC_PRECHECK_Sensors") as SL_MTHLY_GIO_AR_4_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_GIO_AR_4_rerun_check")
    with TaskGroup("SL_MTHLY_GIO_MKT_VAL_EDL") as SL_MTHLY_GIO_MKT_VAL_EDL:
        with TaskGroup("SL_MTHLY_GIO_MKT_VAL_EDL_External_Sensors") as SL_MTHLY_GIO_MKT_VAL_EDL_External_Sensors:
            with TaskGroup("SL_MTHLY_AIP_AR_STG") as SL_MTHLY_AIP_AR_STG:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_AIP_AR")
            with TaskGroup("SL_MTHLY_GIO_TRST_ACCT_EXCL") as SL_MTHLY_GIO_TRST_ACCT_EXCL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            with TaskGroup("SL_MTHLY_IVT_AR_ADL") as SL_MTHLY_IVT_AR_ADL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_IVT_AR")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_GIO_MR_ACCT_EXCL") as SL_MTHLY_GIO_MR_ACCT_EXCL:
        with TaskGroup("SL_MTHLY_GIO_MR_ACCT_EXCL_External_Sensors") as SL_MTHLY_GIO_MR_ACCT_EXCL_External_Sensors:
            with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG_XREF") as SL_MTHLY_GIO_ACCT_HLDNG_XREF:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            with TaskGroup("SL_MTHLY_TBR_AR_STG") as SL_MTHLY_TBR_AR_STG:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_TBR_AR_1")
            with TaskGroup("SL_MTHLY_VLN_AR_ADL") as SL_MTHLY_VLN_AR_ADL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_VLN_AR_2")
            with TaskGroup("SL_MTHLY_VRP_AR_ADL") as SL_MTHLY_VRP_AR_ADL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_VRP_AR_2")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GRS_BAL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_EDL_GRS_BAL") as SL_MTHLY_EDL_GRS_BAL:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_GRS_BAL_External_Sensors") as SL_MTHLY_GRS_BAL_External_Sensors:
        EmptyOperator(task_id="close_process_complete_BSIS_MTHLY_BD_3")
        EmptyOperator(task_id="close_process_complete_CP_MTHLY")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GRS_INSTR_CUSIP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_GSL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_FEP_GLBL") as SL_MTHLY_FEP_GLBL:
        EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_FEP")
    with TaskGroup("SL_MTHLY_GSL_GL") as SL_MTHLY_GSL_GL:
        EmptyOperator(task_id="SL_MTHLY_GSL_GL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_GSL_GL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_GSL_PC_PRECHECK_Sensors") as SL_MTHLY_GSL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GSLCP_SD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_GSLCP_SD_PC_PRECHECK_Sensors") as SL_MTHLY_GSLCP_SD_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_GSLCP_VOL") as SL_MTHLY_GSLCP_VOL:
        EmptyOperator(task_id="SL_MTHLY_GSLCP_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_GSLCP_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_GSLCP_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GSL_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_GSL_CSTNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_GSL_CSTNG_PC_PRECHECK_Sensors") as SL_MTHLY_GSL_CSTNG_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_GSL_VOL") as SL_MTHLY_GSL_VOL:
        EmptyOperator(task_id="SL_MTHLY_GSL_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_GSL_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_GSL_GRS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_GTD_VOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_GTD_EDL_VOL") as SL_MTHLY_GTD_EDL_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_GTD_STG_VOL") as SL_MTHLY_GTD_STG_VOL:
        EmptyOperator(task_id="SL_MTHLY_GTD_STG_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_GTD_STG_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_GTD_STG_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_GTD_VOL_PC_PRECHECK_Sensors") as SL_MTHLY_GTD_VOL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_GTD_VOL_rerun_check")
    with TaskGroup("SL_MTHLY_GTR_EDL_VOL") as SL_MTHLY_GTR_EDL_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_HFM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_HFM_BAL") as SL_MTHLY_HFM_BAL:
        EmptyOperator(task_id="SL_MTHLY_HFM_BAL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_HFM_BAL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_HFM_PC_PRECHECK_Sensors") as SL_MTHLY_HFM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_HFM_CAD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_HFM_CAD_BAL") as SL_MTHLY_HFM_CAD_BAL:
        EmptyOperator(task_id="SL_MTHLY_HFM_CAD_BAL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_HFM_CAD_BAL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_HFM_PC_PRECHECK_Sensors") as SL_MTHLY_HFM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_HFSH",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_HFSH_PRECHECK_Sensors") as SL_MTHLY_HFSH_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_HFSH_VOL") as SL_MTHLY_HFSH_VOL:
        EmptyOperator(task_id="SL_MTHLY_HFSH_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_HFSH_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_HFSH_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_HFS_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_HFS_AR_EDL") as SL_MTHLY_HFS_AR_EDL:
        with TaskGroup("NCW_MTHLY_HFS_AR_EDL_01") as NCW_MTHLY_HFS_AR_EDL_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_HFS_AR_EDL_02") as NCW_MTHLY_HFS_AR_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_MTHLY_HFS_AR_EDL_External_Sensors") as SL_MTHLY_HFS_AR_EDL_External_Sensors:
            with TaskGroup("SL_MTHLY_GIO_TRST_ACCT_EXCL") as SL_MTHLY_GIO_TRST_ACCT_EXCL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_HFS_AR_PC_PRECHECK_Sensors") as SL_MTHLY_HFS_AR_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_HFS_AR_STG") as SL_MTHLY_HFS_AR_STG:
        EmptyOperator(task_id="SL_MTHLY_HFS_AR_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_HFS_AR_STG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_HFS_AR_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_ILA",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_ILA_BK_ENTRS") as SL_MTHLY_ILA_BK_ENTRS:
        EmptyOperator(task_id="SL_MTHLY_ILA_BK_ENTRS_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_ILA_BK_ENTRS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_ILA_External_Sensors") as SL_MTHLY_ILA_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_ILA_VRTNS") as SL_MTHLY_ILA_VRTNS:
        EmptyOperator(task_id="SL_MTHLY_ILA_VRTNS_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_ILA_VRTNS_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_ILA_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_IMAP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_IMAP_PC_PRECHECK_Sensors") as SL_MTHLY_IMAP_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_IMAP_VOL") as SL_MTHLY_IMAP_VOL:
        EmptyOperator(task_id="SL_MTHLY_IMAP_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_IMAP_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_IMAP_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_INT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_INTRDAY_LKP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_IVT_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_LANID",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_DLY_LANID") as SL_DLY_LANID:
        EmptyOperator(task_id="close_process_complete_SL_DLY_LANID")
    with TaskGroup("SL_MTHLY_LANID_PC_PRECHECK_Sensors") as SL_MTHLY_LANID_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_LANID_VOL") as SL_MTHLY_LANID_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="close_process_complete_SL_DLY_LANID")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_LNS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_LNS_DEMO") as SL_MTHLY_LNS_DEMO:
        EmptyOperator(task_id="SL_MTHLY_LNS_DEMO_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_LNS_DEMO_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_LNS_DEMO_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_LNS_External_Sensors") as SL_MTHLY_LNS_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_LOC_VOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_MBO_CCOLL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_MBO_CCOLL_AMER") as SL_MTHLY_MBO_CCOLL_AMER:
        EmptyOperator(task_id="SL_MTHLY_MBO_CCOLL_AMER_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_MBO_CCOLL_AMER_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MBO_CCOLL_APAC") as SL_MTHLY_MBO_CCOLL_APAC:
        EmptyOperator(task_id="SL_MTHLY_MBO_CCOLL_APAC_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_MBO_CCOLL_APAC_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MBO_CCOLL_EMEA") as SL_MTHLY_MBO_CCOLL_EMEA:
        EmptyOperator(task_id="SL_MTHLY_MBO_CCOLL_EMEA_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_MBO_CCOLL_EMEA_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MBO_CCOLL_PC_PRECHECK_Sensors") as SL_MTHLY_MBO_CCOLL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    with TaskGroup("SL_MTHLY_MBO_CCOLL_SG") as SL_MTHLY_MBO_CCOLL_SG:
        EmptyOperator(task_id="SL_MTHLY_MBO_CCOLL_SG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_MBO_CCOLL_SG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_edl_dim_tests")
        EmptyOperator(task_id="run_ge_edl_fact_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_MBO_CCOLL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_MBO_FX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="execute_collibra")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        EmptyOperator(task_id="pull_collibra_datasets")
    with TaskGroup("SL_MTHLY_MBO_FX_AMER") as SL_MTHLY_MBO_FX_AMER:
        EmptyOperator(task_id="SL_MTHLY_MBO_FX_AMER_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_MBO_FX_AMER_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_MBO_FX_AMER_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MBO_FX_APAC") as SL_MTHLY_MBO_FX_APAC:
        EmptyOperator(task_id="SL_MTHLY_MBO_FX_APAC_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_MBO_FX_APAC_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_MBO_FX_APAC_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MBO_FX_EMEA") as SL_MTHLY_MBO_FX_EMEA:
        EmptyOperator(task_id="SL_MTHLY_MBO_FX_EMEA_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_MBO_FX_EMEA_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_MBO_FX_EMEA_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MBO_FX_PC_PRECHECK_Sensors") as SL_MTHLY_MBO_FX_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    EmptyOperator(task_id="SL_MTHLY_MBO_FX_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_MBO_LUX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_MBO_MM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("Collibra") as Collibra:
        with TaskGroup("CollibraDQ") as CollibraDQ:
            EmptyOperator(task_id="execute_collibra")
            EmptyOperator(task_id="pull_collibra_datasets")
        with TaskGroup("Control_M") as Control_M:
            EmptyOperator(task_id="execute_control_M")
            EmptyOperator(task_id="pull_control_M")
        EmptyOperator(task_id="execute_collibra")
        EmptyOperator(task_id="feature_toggle_task_2946443_collibra_integration")
        EmptyOperator(task_id="pull_collibra_datasets")
    with TaskGroup("SL_MTHLY_MBO_MMGLBAL_G") as SL_MTHLY_MBO_MMGLBAL_G:
        with TaskGroup("SL_MTHLY_MBO_MMGLBAL") as SL_MTHLY_MBO_MMGLBAL:
            with TaskGroup("NCW_MTHLY_MBO_MMGLBAL_AMER_STG_01") as NCW_MTHLY_MBO_MMGLBAL_AMER_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_source_file_sensor")
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMGLBAL_APAC_STG_01") as NCW_MTHLY_MBO_MMGLBAL_APAC_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_source_file_sensor")
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMGLBAL_EMEA_STG_01") as NCW_MTHLY_MBO_MMGLBAL_EMEA_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_source_file_sensor")
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMGLBAL_SG_STG_01") as NCW_MTHLY_MBO_MMGLBAL_SG_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMGLBAL_source_file_sensor")
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="check_adl_fact_task_status")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("SL_MTHLY_MBO_MMGLBAL_EDL") as SL_MTHLY_MBO_MMGLBAL_EDL:
            with TaskGroup("NCW_MTHLY_MBO_MMGLBAL_EDL_01") as NCW_MTHLY_MBO_MMGLBAL_EDL_01:
                EmptyOperator(task_id="fact_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_edl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMGLBAL_EDL_02") as NCW_MTHLY_MBO_MMGLBAL_EDL_02:
                EmptyOperator(task_id="fact_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_edl_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="check_edl_task_status")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_MBO_MMTRX") as SL_MTHLY_MBO_MMTRX:
        with TaskGroup("SL_MTHLY_MBO_MMTRX_ADL") as SL_MTHLY_MBO_MMTRX_ADL:
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_AMER_ADL_01") as NCW_MTHLY_MBO_MMTRX_AMER_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_APAC_ADL_01") as NCW_MTHLY_MBO_MMTRX_APAC_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_EMEA_ADL_01") as NCW_MTHLY_MBO_MMTRX_EMEA_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_SG_ADL_01") as NCW_MTHLY_MBO_MMTRX_SG_ADL_01:
                EmptyOperator(task_id="adl_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_adl_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="check_adl_task_status")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("SL_MTHLY_MBO_MMTRX_EDL") as SL_MTHLY_MBO_MMTRX_EDL:
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_EDL_01") as NCW_MTHLY_MBO_MMTRX_EDL_01:
                EmptyOperator(task_id="dim_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_dim_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_EDL_02") as NCW_MTHLY_MBO_MMTRX_EDL_02:
                EmptyOperator(task_id="fact_load")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="run_ge_edl_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="check_edl_task_status")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
        with TaskGroup("SL_MTHLY_MBO_MMTRX_STG") as SL_MTHLY_MBO_MMTRX_STG:
            with TaskGroup("NCW_MTHLY_MBO_MMPD_AMER_STG_01") as NCW_MTHLY_MBO_MMPD_AMER_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMPD_APAC_STG_01") as NCW_MTHLY_MBO_MMPD_APAC_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMPD_EMEA_STG_01") as NCW_MTHLY_MBO_MMPD_EMEA_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_AMER_STG_01") as NCW_MTHLY_MBO_MMTRX_AMER_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_APAC_STG_01") as NCW_MTHLY_MBO_MMTRX_APAC_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_EMEA_STG_01") as NCW_MTHLY_MBO_MMTRX_EMEA_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            with TaskGroup("NCW_MTHLY_MBO_MMTRX_SG_STG_01") as NCW_MTHLY_MBO_MMTRX_SG_STG_01:
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_copy_source_file_adls")
                EmptyOperator(task_id="SL_MTHLY_MBO_MMTRX_STG_source_file_sensor")
                EmptyOperator(task_id="delete_source_file")
                EmptyOperator(task_id="ingest_data")
                EmptyOperator(task_id="populate_asof_date")
                EmptyOperator(task_id="prepare_data")
                EmptyOperator(task_id="run_ge_s1_tests")
                EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="close_process")
            EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_MBO_MM_PC_PRECHECK_Sensors") as SL_MTHLY_MBO_MM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    EmptyOperator(task_id="SL_MTHLY_MBO_MM_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_MBO_NOSTRO_EOM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_MCL_CMM_RSK",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_MCL_CRE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_MID_DEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_MID_DEP_AVG_EUR") as SL_MTHLY_MID_DEP_AVG_EUR:
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_AVG_EUR_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_AVG_EUR_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MID_DEP_AVG_GBP") as SL_MTHLY_MID_DEP_AVG_GBP:
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_AVG_GBP_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_AVG_GBP_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MID_DEP_AVG_USD") as SL_MTHLY_MID_DEP_AVG_USD:
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_AVG_USD_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_AVG_USD_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MID_DEP_FACT") as SL_MTHLY_MID_DEP_FACT:
        with TaskGroup("NCW_MTHLY_MID_DEP_BAL_EDL_01") as NCW_MTHLY_MID_DEP_BAL_EDL_01:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_MID_DEP_BAL_EDL_02") as NCW_MTHLY_MID_DEP_BAL_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="check_edl_task_status")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_MID_DEP_INSTRMNT_ADL") as SL_MTHLY_MID_DEP_INSTRMNT_ADL:
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MID_DEP_INSTRMNT_EDL") as SL_MTHLY_MID_DEP_INSTRMNT_EDL:
        EmptyOperator(task_id="dim_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_dim_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MID_DEP_PC_PRECHECK_Sensors") as SL_MTHLY_MID_DEP_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    EmptyOperator(task_id="SL_MTHLY_MID_DEP_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_MID_DEP_NTBANK",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_MID_DEP_NTBANK_PC_PRECHECK_Sensors") as SL_MTHLY_MID_DEP_NTBANK_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    with TaskGroup("SL_MTHLY_MID_DEP_NTBANK_PD") as SL_MTHLY_MID_DEP_NTBANK_PD:
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_NTBANK_PD_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_NTBANK_PD_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_NTBANK_PD_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_MID_DEP_NTBANK_PD_SIG") as SL_MTHLY_MID_DEP_NTBANK_PD_SIG:
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_NTBANK_PD_SIG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_NTBANK_PD_SIG_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_MID_DEP_NTBANK_PD_SIG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_MID_DEP_NTBANK_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_MID_LNS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_MID_LNS_PC_PRECHECK_Sensors") as SL_MTHLY_MID_LNS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    with TaskGroup("SL_MTHLY_MID_LNS_VOL") as SL_MTHLY_MID_LNS_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_MID_MUC_DEP",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_NBS_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NBS_AR_EDL") as SL_MTHLY_NBS_AR_EDL:
        with TaskGroup("NCW_MTHLY_NBS_AR_EDL_01") as NCW_MTHLY_NBS_AR_EDL_01:
            with TaskGroup("SL_MTHLY_NBS_AR_EDL_External_Sensors") as SL_MTHLY_NBS_AR_EDL_External_Sensors:
                with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG_XREF") as SL_MTHLY_GIO_ACCT_HLDNG_XREF:
                    EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
                with TaskGroup("SL_MTHLY_GIO_TRST_ACCT_EXCL") as SL_MTHLY_GIO_TRST_ACCT_EXCL:
                    EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_NBS_AR_EDL_02") as NCW_MTHLY_NBS_AR_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_NBS_AR_PC_PRECHECK_Sensors") as SL_MTHLY_NBS_AR_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_NBS_AR_STG") as SL_MTHLY_NBS_AR_STG:
        EmptyOperator(task_id="SL_MTHLY_NBS_AR_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_NBS_AR_STG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_NBS_AR_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_NBS_IS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NBS_CLNT") as SL_MTHLY_NBS_CLNT:
        EmptyOperator(task_id="SL_MTHLY_NBS_CLNT_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_NBS_CLNT_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_NBS_CLNT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_NBS_CLNT_AVT") as SL_MTHLY_NBS_CLNT_AVT:
        EmptyOperator(task_id="SL_MTHLY_NBS_CLNT_AVT_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_NBS_CLNT_AVT_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_NBS_CLNT_AVT_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_NBS_CLNT_EDL") as SL_MTHLY_NBS_CLNT_EDL:
        with TaskGroup("NCW_MTHLY_NBS_CLNT_EDL_01") as NCW_MTHLY_NBS_CLNT_EDL_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_NBS_CLNT_EDL_02") as NCW_MTHLY_NBS_CLNT_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_NBS_CLNT_FIN_EDL") as SL_MTHLY_NBS_CLNT_FIN_EDL:
        with TaskGroup("NCW_MTHLY_NBS_CLNT_FIN_EDL_01") as NCW_MTHLY_NBS_CLNT_FIN_EDL_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_NBS_CLNT_FIN_EDL_02") as NCW_MTHLY_NBS_CLNT_FIN_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_NBS_IS_External_Sensors") as SL_MTHLY_NBS_IS_External_Sensors:
        with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG") as SL_MTHLY_GIO_ACCT_HLDNG:
            EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_NBS_IS_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_NCW_VDR_FOREIGN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_NCW_VDR_TBR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_NFS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NFS_ACCT") as SL_MTHLY_NFS_ACCT:
        with TaskGroup("NCW_MTHLY_NFS_NOREB_STG_01") as NCW_MTHLY_NFS_NOREB_STG_01:
            EmptyOperator(task_id="SL_MTHLY_NFS_ACCT_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_NFS_ACCT_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_NFS_RAS_RPT_STG_01") as NCW_MTHLY_NFS_RAS_RPT_STG_01:
            EmptyOperator(task_id="SL_MTHLY_NFS_ACCT_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_NFS_ACCT_source_file_sensor")
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_MTHLY_NFS_FIN_PRDCT_EXCEPTION") as SL_MTHLY_NFS_FIN_PRDCT_EXCEPTION:
            EmptyOperator(task_id="sp_load")
            EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="undefined_product_check")
        EmptyOperator(task_id="check_nfs_task_status")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_NFS_EDL") as SL_MTHLY_NFS_EDL:
        with TaskGroup("NCW_MTHLY_NFS_EDL_01") as NCW_MTHLY_NFS_EDL_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_NFS_EDL_02") as NCW_MTHLY_NFS_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_NFS_PC_PRECHECK_Sensors") as SL_MTHLY_NFS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_NFS_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="defer_to_next_bd")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_NFSC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NFSC_PC_PRECHECK_Sensors") as SL_MTHLY_NFSC_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_NFSC_VOL") as SL_MTHLY_NFSC_VOL:
        EmptyOperator(task_id="SL_MTHLY_NFSC_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_NFSC_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_NFSC_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_NFS_AFL_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NFS_AFL_1_PC_PRECHECK_Sensors") as SL_MTHLY_NFS_AFL_1_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_NFS_AFL_STG") as SL_MTHLY_NFS_AFL_STG:
        EmptyOperator(task_id="SL_MTHLY_NFS_AFL_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_NFS_AFL_STG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_NFS_AFL_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_NFS_AR_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_NFS_AR_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_NFS_VOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_NTAM_APAC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NTAM_APAC_PC_PRECHECK_Sensors") as SL_MTHLY_NTAM_APAC_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_NTAM_APAC_VOL") as SL_MTHLY_NTAM_APAC_VOL:
        EmptyOperator(task_id="SL_MTHLY_NTAM_APAC_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_NTAM_APAC_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_NTAM_APAC_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_NTAM_JPN",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NTAM_JPN_PC_PRECHECK_Sensors") as SL_MTHLY_NTAM_JPN_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_NTAM_JPN_VOL") as SL_MTHLY_NTAM_JPN_VOL:
        EmptyOperator(task_id="SL_MTHLY_NTAM_JPN_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_NTAM_JPN_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_NTGAM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NTGAM_PC_PRECHECK_Sensors") as SL_MTHLY_NTGAM_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_NTGAM_VOL") as SL_MTHLY_NTGAM_VOL:
        EmptyOperator(task_id="SL_MTHLY_NTGAM_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_NTGAM_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_NTGAM_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_NTGIR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NTGIR_PC_PRECHECK_Sensors") as SL_MTHLY_NTGIR_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_NTGIR_VOL") as SL_MTHLY_NTGIR_VOL:
        EmptyOperator(task_id="SL_MTHLY_NTGIR_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_NTGIR_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_NTGIR_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_OMNIUM_GTX_SD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_OMNIUM_GTX_SD_PC_PRECHECK_Sensors") as SL_MTHLY_OMNIUM_GTX_SD_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_OMNIUM_GTX_VOL") as SL_MTHLY_OMNIUM_GTX_VOL:
        EmptyOperator(task_id="SL_MTHLY_OMNIUM_GTX_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_OMNIUM_GTX_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_OREO",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_OTH",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_OTH_PC_PRECHECK_Sensors") as SL_MTHLY_OTH_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_OTH_VOL") as SL_MTHLY_OTH_VOL:
        EmptyOperator(task_id="SL_MTHLY_OTH_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_OTH_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_PACE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_PACE_PC_PRECHECK_Sensors") as SL_MTHLY_PACE_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_PACE_VOL") as SL_MTHLY_PACE_VOL:
        EmptyOperator(task_id="SL_MTHLY_PACE_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_PACE_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_PACE_INDX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_PED",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_PED_PC_PRECHECK_Sensors") as SL_MTHLY_PED_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_PED_VOL") as SL_MTHLY_PED_VOL:
        EmptyOperator(task_id="SL_MTHLY_PED_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_PED_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_PED_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_PEG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_PIN_CLT_DEP_PRC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_PIN_CLTPRC_ADL") as SL_MTHLY_PIN_CLTPRC_ADL:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_PIN_CLTPRC_AUD_STG") as SL_MTHLY_PIN_CLTPRC_AUD_STG:
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_AUD_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_AUD_STG_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_AUD_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_PIN_CLTPRC_EUR_STG") as SL_MTHLY_PIN_CLTPRC_EUR_STG:
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_EUR_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_EUR_STG_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_EUR_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_PIN_CLTPRC_GBP_STG") as SL_MTHLY_PIN_CLTPRC_GBP_STG:
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_GBP_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_GBP_STG_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_GBP_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_PIN_CLTPRC_USD_STG") as SL_MTHLY_PIN_CLTPRC_USD_STG:
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_USD_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_USD_STG_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_PIN_CLTPRC_USD_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_PIN_CLT_DEP_PRC_External_Sensors") as SL_MTHLY_PIN_CLT_DEP_PRC_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    with TaskGroup("ncw_control_m_jobs") as ncw_control_m_jobs:
        EmptyOperator(task_id="execute_ctm_job")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_PPM_EDL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_PPM_STG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_PPM_HOURS_STG") as SL_MTHLY_PPM_HOURS_STG:
        EmptyOperator(task_id="SL_MTHLY_PPM_HOURS_STG_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_PPM_HOURS_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_PPM_STG_PC_PRECHECK_Sensors") as SL_MTHLY_PPM_STG_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_203")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_PP_VOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_PP_ACCTS_VOL") as SL_MTHLY_PP_ACCTS_VOL:
        EmptyOperator(task_id="SL_MTHLY_PP_ACCTS_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_PP_ACCTS_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_PP_ACCTS_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load_edl_01")
        EmptyOperator(task_id="fact_load_edl_02")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_01_tests")
        EmptyOperator(task_id="run_ge_edl_02_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_PP_VOL_PC_PRECHECK_Sensors") as SL_MTHLY_PP_VOL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_PSBIL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_PSBIL_EDL") as SL_MTHLY_PSBIL_EDL:
        with TaskGroup("NCW_MTHLY_PSBIL_EDL_01") as NCW_MTHLY_PSBIL_EDL_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_PSBIL_EDL_02") as NCW_MTHLY_PSBIL_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_PSBIL_PC_PRECHECK_Sensors") as SL_MTHLY_PSBIL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_PSBIL_RVNU") as SL_MTHLY_PSBIL_RVNU:
        EmptyOperator(task_id="SL_MTHLY_PSBIL_RVNU_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_PSBIL_RVNU_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_PSBIL_RVNU_source_file_sensor")
        with TaskGroup("SL_MTHLY_PSBIL_UNDFND_PRDCT_CHECK") as SL_MTHLY_PSBIL_UNDFND_PRDCT_CHECK:
            EmptyOperator(task_id="sp_load")
            EmptyOperator(task_id="undefined_product_check")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_PSBIL_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="defer_to_next_bd")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_PSF",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_PSF_ADL") as SL_MTHLY_PSF_ADL:
        with TaskGroup("NCW_MTHLY_PSF_TRST_FEE_ADL_01") as NCW_MTHLY_PSF_TRST_FEE_ADL_01:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_PSF_UNDFNDPROD_01") as NCW_MTHLY_PSF_UNDFNDPROD_01:
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="sp_load")
            EmptyOperator(task_id="success_email_notification")
            EmptyOperator(task_id="undefined_product_check")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_PSF_EDL") as SL_MTHLY_PSF_EDL:
        with TaskGroup("NCW_MTHLY_PSF_EDL_01") as NCW_MTHLY_PSF_EDL_01:
            EmptyOperator(task_id="dim_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_dim_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_PSF_EDL_02") as NCW_MTHLY_PSF_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_PSF_PC_PRECHECK_Sensors") as SL_MTHLY_PSF_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_PSF_STG") as SL_MTHLY_PSF_STG:
        with TaskGroup("NCW_MTHLY_PSF_EUR_STG_01") as NCW_MTHLY_PSF_EUR_STG_01:
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_PSF_GBP_STG_01") as NCW_MTHLY_PSF_GBP_STG_01:
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_PSF_HKG_STG_01") as NCW_MTHLY_PSF_HKG_STG_01:
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_PSF_JPN_STG_01") as NCW_MTHLY_PSF_JPN_STG_01:
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_PSF_SGP_STG_01") as NCW_MTHLY_PSF_SGP_STG_01:
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_PSF_USD_STG_01") as NCW_MTHLY_PSF_USD_STG_01:
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_copy_source_file_adls")
            EmptyOperator(task_id="SL_MTHLY_PSF_STG_source_file_sensor")
            EmptyOperator(task_id="delete_source_file")
            EmptyOperator(task_id="ingest_data")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="prepare_data")
            EmptyOperator(task_id="run_ge_s1_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="SL_MTHLY_PSF_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="defer_to_next_bd")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_PSM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_QRM_RCE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_QRM_FRCST_AUDIT") as SL_MTHLY_QRM_FRCST_AUDIT:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_QRM_RCE_External_Sensors") as SL_MTHLY_QRM_RCE_External_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_QRM_RCE_rerun_check")
    with TaskGroup("SL_MTHLY_QRM_VAL_SHOCK") as SL_MTHLY_QRM_VAL_SHOCK:
        EmptyOperator(task_id="generic_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_generic_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_RAS_VOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_RCT_ASST_RPT",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_RCT_TRST_FEE",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_REV_ALLC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_SAP_ALLC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_SFI_CSTNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_SFK",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_SFK_PC_PRECHECK_Sensors") as SL_MTHLY_SFK_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_SFK_VOL") as SL_MTHLY_SFK_VOL:
        EmptyOperator(task_id="SL_MTHLY_SFK_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_SFK_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_SFKC_VOL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_SFKC_EDL_VOL") as SL_MTHLY_SFKC_EDL_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_SFKC_STG_VOL") as SL_MTHLY_SFKC_STG_VOL:
        EmptyOperator(task_id="SL_MTHLY_SFKC_STG_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_SFKC_STG_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_SFKC_VOL_PC_PRECHECK_Sensors") as SL_MTHLY_SFKC_VOL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_SFKC_VOL_rerun_check")
    with TaskGroup("SL_MTHLY_SFKN_EDL_VOL") as SL_MTHLY_SFKN_EDL_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_SFKR_EDL_VOL") as SL_MTHLY_SFKR_EDL_VOL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_SLT_TIC_AR_01",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NRA_ACCT_STG") as SL_MTHLY_NRA_ACCT_STG:
        EmptyOperator(task_id="SL_MTHLY_NRA_ACCT_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_NRA_ACCT_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_SLT_TIC_AR_01_PC_PRECHECK_Sensors") as SL_MTHLY_SLT_TIC_AR_01_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_SLT_TIC_AR_02",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_NRA_ACCT_GLBL") as SL_MTHLY_NRA_ACCT_GLBL:
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_SLT_TIC_AR_02_External_Sensors") as SL_MTHLY_SLT_TIC_AR_02_External_Sensors:
        with TaskGroup("SL_MTHLY_NRA_ACCT_STG") as SL_MTHLY_NRA_ACCT_STG:
            EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_SLT_TIC_AR_01")
        EmptyOperator(task_id="close_process_complete_AFL_MTHLY")
    with TaskGroup("SL_MTHLY_SLT_TIC_AR_02_PC_PRECHECK_Sensors") as SL_MTHLY_SLT_TIC_AR_02_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_SUBC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_SUBC_PC_PRECHECK_Sensors") as SL_MTHLY_SUBC_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_SUBC_VOL") as SL_MTHLY_SUBC_VOL:
        EmptyOperator(task_id="SL_MTHLY_SUBC_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_SUBC_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_SUBC_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_SVC",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_SVC_PC_PRECHECK_Sensors") as SL_MTHLY_SVC_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_SVC_VOL") as SL_MTHLY_SVC_VOL:
        EmptyOperator(task_id="SL_MTHLY_SVC_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_SVC_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_SVC_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_SVCHD_SD",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_SVCHD_SD_PC_PRECHECK_Sensors") as SL_MTHLY_SVCHD_SD_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_SVCHD_VOL") as SL_MTHLY_SVCHD_VOL:
        EmptyOperator(task_id="SL_MTHLY_SVCHD_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_SVCHD_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_SVCHD_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_SWAPS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_SWAPS_PC_PRECHECK_Sensors") as SL_MTHLY_SWAPS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_SWAPS_VOL") as SL_MTHLY_SWAPS_VOL:
        EmptyOperator(task_id="SL_MTHLY_SWAPS_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_SWAPS_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load_01")
        EmptyOperator(task_id="fact_load_02")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_edl_tests__1")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TAX",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_TAX_PC_PRECHECK_Sensors") as SL_MTHLY_TAX_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_201")
    with TaskGroup("SL_MTHLY_TAX_VOL") as SL_MTHLY_TAX_VOL:
        EmptyOperator(task_id="SL_MTHLY_TAX_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_TAX_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TAX_RECL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_TAX_RECL_PC_PRECHECK_Sensors") as SL_MTHLY_TAX_RECL_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_TAX_RECL_VOL") as SL_MTHLY_TAX_RECL_VOL:
        EmptyOperator(task_id="SL_MTHLY_TAX_RECL_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_TAX_RECL_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TBR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_TBR_DEPGL") as SL_MTHLY_TBR_DEPGL:
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_TBR_FND") as SL_MTHLY_TBR_FND:
        EmptyOperator(task_id="SL_MTHLY_TBR_FND_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_TBR_FND_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_TBR_FND_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_TBR_PC_PRECHECK_Sensors") as SL_MTHLY_TBR_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_TBR_SWEEPVHCL") as SL_MTHLY_TBR_SWEEPVHCL:
        EmptyOperator(task_id="SL_MTHLY_TBR_SWEEPVHCL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_TBR_SWEEPVHCL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_TBR_SWEEPVHCL_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="SL_MTHLY_TBR_rerun_check")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="close_process_complete_SL_DLY_TBR")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TBRGL_CSTNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_TBRNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_TBRNG_PC_PRECHECK_Sensors") as SL_MTHLY_TBRNG_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_TBRNG_VOL") as SL_MTHLY_TBRNG_VOL:
        EmptyOperator(task_id="SL_MTHLY_TBRNG_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_TBRNG_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_TBRNG_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TBRSW",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_TBRSW_PC_PRECHECK_Sensors") as SL_MTHLY_TBRSW_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_TBRSW_VOL") as SL_MTHLY_TBRSW_VOL:
        EmptyOperator(task_id="SL_MTHLY_TBRSW_VOL_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_TBRSW_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_TBRSW_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TBR_AR_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_TBR_AR_1_PC_PRECHECK_Sensors") as SL_MTHLY_TBR_AR_1_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_TBR_AR_STG") as SL_MTHLY_TBR_AR_STG:
        EmptyOperator(task_id="SL_MTHLY_TBR_AR_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_TBR_AR_STG_source_file_sensor")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TBR_AR_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_TBS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_TBS_COSTNG",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_TFS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_TFS_PC_PRECHECK_Sensors") as SL_MTHLY_TFS_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_TFS_VOL") as SL_MTHLY_TFS_VOL:
        EmptyOperator(task_id="SL_MTHLY_TFS_VOL_copy_source_file_adls")
        EmptyOperator(task_id="SL_MTHLY_TFS_VOL_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_TRN_AFL_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_TRN_AFL_1_PC_PRECHECK_Sensors") as SL_MTHLY_TRN_AFL_1_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_TRN_AFL_1_rerun_check")
    with TaskGroup("SL_MTHLY_TRN_AFL_ADL") as SL_MTHLY_TRN_AFL_ADL:
        with TaskGroup("NCW_MTHLY_TRN_AFL_ADL_01") as NCW_MTHLY_TRN_AFL_ADL_01:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_TRN_AFL_ADL_02") as NCW_MTHLY_TRN_AFL_ADL_02:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_TRN_AFL_ADL_03") as NCW_MTHLY_TRN_AFL_ADL_03:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_TRN_AFL_ADL_04") as NCW_MTHLY_TRN_AFL_ADL_04:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_TRN_AFL_ADL_05") as NCW_MTHLY_TRN_AFL_ADL_05:
            EmptyOperator(task_id="adl_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_adl_tests")
            EmptyOperator(task_id="success_email_notification")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    with TaskGroup("SL_MTHLY_TRN_AFL_STG") as SL_MTHLY_TRN_AFL_STG:
        EmptyOperator(task_id="SL_MTHLY_TRN_AFL_STG_copy_source_file")
        EmptyOperator(task_id="SL_MTHLY_TRN_AFL_STG_source_file_sensor")
        EmptyOperator(task_id="delete_source_file")
        EmptyOperator(task_id="ingest_data")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="prepare_data")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_s1_tests")
        EmptyOperator(task_id="sp_load")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TRN_AFL_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_TRN_AFL_2_PC_PRECHECK_Sensors") as SL_MTHLY_TRN_AFL_2_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    with TaskGroup("SL_MTHLY_TRN_AFL_EDL") as SL_MTHLY_TRN_AFL_EDL:
        with TaskGroup("NCW_MTHLY_TRN_AFL_EDL_01") as NCW_MTHLY_TRN_AFL_EDL_01:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_TRN_AFL_EDL_02") as NCW_MTHLY_TRN_AFL_EDL_02:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_TRN_AFL_EDL_03") as NCW_MTHLY_TRN_AFL_EDL_03:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_TRN_AFL_EDL_04") as NCW_MTHLY_TRN_AFL_EDL_04:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("NCW_MTHLY_TRN_AFL_EDL_05") as NCW_MTHLY_TRN_AFL_EDL_05:
            EmptyOperator(task_id="fact_load")
            EmptyOperator(task_id="populate_asof_date")
            EmptyOperator(task_id="run_ge_edl_tests")
            EmptyOperator(task_id="success_email_notification")
        with TaskGroup("SL_MTHLY_TRN_AFL_EDL_External_Sensors") as SL_MTHLY_TRN_AFL_EDL_External_Sensors:
            with TaskGroup("AFL_MTHLY_FLOW_DSTN_EDL") as AFL_MTHLY_FLOW_DSTN_EDL:
                EmptyOperator(task_id="close_process_complete_AFL_MTHLY_FLOW_DSTN")
            with TaskGroup("SL_MTHLY_ETA_AFL_EDL") as SL_MTHLY_ETA_AFL_EDL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_ETA_AFL_2")
            with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG_XREF") as SL_MTHLY_GIO_ACCT_HLDNG_XREF:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            with TaskGroup("SL_MTHLY_GIO_ACCT_SO_PG") as SL_MTHLY_GIO_ACCT_SO_PG:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_3")
            with TaskGroup("SL_MTHLY_GIO_TRST_ACCT_EXCL") as SL_MTHLY_GIO_TRST_ACCT_EXCL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            with TaskGroup("SL_MTHLY_TRN_AFL_ADL") as SL_MTHLY_TRN_AFL_ADL:
                EmptyOperator(task_id="close_process_complete_SL_MTHLY_TRN_AFL_1")
        EmptyOperator(task_id="close_process")
        EmptyOperator(task_id="open_process")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_TRP_AFL_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_TRP_AFL_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_TRS_SWAPS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_VGR_AR",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_VLN_AR_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_VLN_AR_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup("SL_MTHLY_VLN_AR_2_PC_PRECHECK_Sensors") as SL_MTHLY_VLN_AR_2_PC_PRECHECK_Sensors:
        EmptyOperator(task_id="close_process_complete_PC_MTHLY_200")
    EmptyOperator(task_id="SL_MTHLY_VLN_AR_2_rerun_check")
    with TaskGroup("SL_MTHLY_VLN_AR_ADL") as SL_MTHLY_VLN_AR_ADL:
        with TaskGroup("SL_MTHLY_VLN_AR_ADL_External_Sensors") as SL_MTHLY_VLN_AR_ADL_External_Sensors:
            with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG_XREF") as SL_MTHLY_GIO_ACCT_HLDNG_XREF:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            with TaskGroup("SL_MTHLY_GIO_ACCT_SO_PG") as SL_MTHLY_GIO_ACCT_SO_PG:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_3")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_VLN_AR_1")
        EmptyOperator(task_id="adl_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_adl_tests")
        EmptyOperator(task_id="success_email_notification")
    with TaskGroup("SL_MTHLY_VLN_AR_EDL") as SL_MTHLY_VLN_AR_EDL:
        with TaskGroup("SL_MTHLY_VLN_AR_EDL_External_Sensors") as SL_MTHLY_VLN_AR_EDL_External_Sensors:
            with TaskGroup("SL_MTHLY_AIP_AR_STG") as SL_MTHLY_AIP_AR_STG:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_AIP_AR")
            with TaskGroup("SL_MTHLY_GCS_AR_DEPOT") as SL_MTHLY_GCS_AR_DEPOT:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GCS_AR")
            with TaskGroup("SL_MTHLY_GIO_ACCT_HLDNG_XREF") as SL_MTHLY_GIO_ACCT_HLDNG_XREF:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            with TaskGroup("SL_MTHLY_GIO_ACCT_SO_PG") as SL_MTHLY_GIO_ACCT_SO_PG:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_3")
            with TaskGroup("SL_MTHLY_GIO_TRST_ACCT_EXCL") as SL_MTHLY_GIO_TRST_ACCT_EXCL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_GIO_AR_1")
            with TaskGroup("SL_MTHLY_IVT_AR_ADL") as SL_MTHLY_IVT_AR_ADL:
                EmptyOperator(task_id="rcc_close_process_complete_SL_MTHLY_IVT_AR")
            with TaskGroup("SL_MTHLY_VRP_AR_STG") as SL_MTHLY_VRP_AR_STG:
                EmptyOperator(task_id="close_process_complete_SL_MTHLY_VRP_AR_1")
            EmptyOperator(task_id="close_process_complete_SL_MTHLY_TBR_AR_2")
        EmptyOperator(task_id="fact_load")
        EmptyOperator(task_id="populate_asof_date")
        EmptyOperator(task_id="rcc_close_process")
        EmptyOperator(task_id="rcc_open_process")
        EmptyOperator(task_id="run_ge_edl_tests")
        EmptyOperator(task_id="success_email_notification")
    EmptyOperator(task_id="close_process")
    EmptyOperator(task_id="email_notif_op")
    EmptyOperator(task_id="open_process")

with DAG(
    dag_id="SL_MTHLY_VRP_AR_1",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_MTHLY_VRP_AR_2",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_QTRLY_FHLB",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_QTRLY_GRS",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_QTRLY_MGR_BURST_CNTRL",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="SL_QTRLY_MRM",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")

with DAG(
    dag_id="ops_adls_file_migration",
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="placeholder")
