"""
MWAA Serverless supported operators and YAML schema.

Operators sourced from the official allowlist:
https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/operators.html

IMPORTANT: MWAA Serverless requires the FULLY QUALIFIED operator path in the
`operator:` field. Short names (e.g. "S3ListOperator") are rejected by the
service with: "Task 'x' 's operator 'S3ListOperator' is not supported".
The short names in SUPPORTED_OPERATORS exist only so tooling can resolve a
friendly name to the FQN that must be emitted.
"""

SUPPORTED_OPERATORS = {
    # ── Core / standard provider ──
    "EmptyOperator": "airflow.operators.empty.EmptyOperator",
    "PythonOperator": "airflow.providers.standard.operators.python.PythonOperator",
    "BashOperator": "airflow.providers.standard.operators.bash.BashOperator",
    # ── S3 ──
    "S3CreateBucketOperator": "airflow.providers.amazon.aws.operators.s3.S3CreateBucketOperator",
    "S3DeleteBucketOperator": "airflow.providers.amazon.aws.operators.s3.S3DeleteBucketOperator",
    "S3GetBucketTaggingOperator": "airflow.providers.amazon.aws.operators.s3.S3GetBucketTaggingOperator",
    "S3PutBucketTaggingOperator": "airflow.providers.amazon.aws.operators.s3.S3PutBucketTaggingOperator",
    "S3DeleteBucketTaggingOperator": "airflow.providers.amazon.aws.operators.s3.S3DeleteBucketTaggingOperator",
    "S3CopyObjectOperator": "airflow.providers.amazon.aws.operators.s3.S3CopyObjectOperator",
    "S3CreateObjectOperator": "airflow.providers.amazon.aws.operators.s3.S3CreateObjectOperator",
    "S3DeleteObjectsOperator": "airflow.providers.amazon.aws.operators.s3.S3DeleteObjectsOperator",
    "S3ListOperator": "airflow.providers.amazon.aws.operators.s3.S3ListOperator",
    "S3ListPrefixesOperator": "airflow.providers.amazon.aws.operators.s3.S3ListPrefixesOperator",
    "S3KeySensor": "airflow.providers.amazon.aws.sensors.s3.S3KeySensor",
    "S3KeysUnchangedSensor": "airflow.providers.amazon.aws.sensors.s3.S3KeysUnchangedSensor",
    # ── S3 Tables ──
    "S3TablesCreateTableBucketOperator": "airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateTableBucketOperator",
    "S3TablesCreateNamespaceOperator": "airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateNamespaceOperator",
    "S3TablesCreateTableOperator": "airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateTableOperator",
    "S3TablesDeleteTableBucketOperator": "airflow.providers.amazon.aws.operators.s3_tables.S3TablesDeleteTableBucketOperator",
    "S3TablesDeleteNamespaceOperator": "airflow.providers.amazon.aws.operators.s3_tables.S3TablesDeleteNamespaceOperator",
    "S3TablesDeleteTableOperator": "airflow.providers.amazon.aws.operators.s3_tables.S3TablesDeleteTableOperator",
    # ── S3 Vectors ──
    "S3VectorsCreateVectorBucketOperator": "airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsCreateVectorBucketOperator",
    "S3VectorsCreateIndexOperator": "airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsCreateIndexOperator",
    "S3VectorsDeleteIndexOperator": "airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsDeleteIndexOperator",
    "S3VectorsDeleteVectorBucketOperator": "airflow.providers.amazon.aws.operators.s3_vectors.S3VectorsDeleteVectorBucketOperator",
    # ── MWAA Serverless (nested workflows) ──
    "MwaaServerlessCreateWorkflowOperator": "airflow.providers.amazon.aws.operators.mwaa_serverless.MwaaServerlessCreateWorkflowOperator",
    "MwaaServerlessStartWorkflowRunOperator": "airflow.providers.amazon.aws.operators.mwaa_serverless.MwaaServerlessStartWorkflowRunOperator",
    # ── Glue ──
    "GlueJobOperator": "airflow.providers.amazon.aws.operators.glue.GlueJobOperator",
    "GlueDataQualityOperator": "airflow.providers.amazon.aws.operators.glue.GlueDataQualityOperator",
    "GlueDataQualityRuleSetEvaluationRunOperator": "airflow.providers.amazon.aws.operators.glue.GlueDataQualityRuleSetEvaluationRunOperator",
    "GlueDataQualityRuleRecommendationRunOperator": "airflow.providers.amazon.aws.operators.glue.GlueDataQualityRuleRecommendationRunOperator",
    "GlueDataBrewStartJobOperator": "airflow.providers.amazon.aws.operators.glue_databrew.GlueDataBrewStartJobOperator",
    "GlueCrawlerOperator": "airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerOperator",
    "GlueJobSensor": "airflow.providers.amazon.aws.sensors.glue.GlueJobSensor",
    "GlueDataQualityRuleSetEvaluationRunSensor": "airflow.providers.amazon.aws.sensors.glue.GlueDataQualityRuleSetEvaluationRunSensor",
    "GlueDataQualityRuleRecommendationRunSensor": "airflow.providers.amazon.aws.sensors.glue.GlueDataQualityRuleRecommendationRunSensor",
    "GlueCatalogPartitionSensor": "airflow.providers.amazon.aws.sensors.glue_catalog_partition.GlueCatalogPartitionSensor",
    "GlueCrawlerSensor": "airflow.providers.amazon.aws.sensors.glue_crawler.GlueCrawlerSensor",
    # ── Glue Data Catalog ──
    "GlueCatalogCreateDatabaseOperator": "airflow.providers.amazon.aws.operators.glue_catalog.GlueCatalogCreateDatabaseOperator",
    "GlueCatalogCreateTableOperator": "airflow.providers.amazon.aws.operators.glue_catalog.GlueCatalogCreateTableOperator",
    "GlueCatalogDeleteDatabaseOperator": "airflow.providers.amazon.aws.operators.glue_catalog.GlueCatalogDeleteDatabaseOperator",
    "GlueCatalogDeleteTableOperator": "airflow.providers.amazon.aws.operators.glue_catalog.GlueCatalogDeleteTableOperator",
    # ── Athena ──
    "AthenaOperator": "airflow.providers.amazon.aws.operators.athena.AthenaOperator",
    "AthenaSensor": "airflow.providers.amazon.aws.sensors.athena.AthenaSensor",
    # ── Bedrock ──
    "BedrockInvokeModelOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockInvokeModelOperator",
    "BedrockCustomizeModelOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockCustomizeModelOperator",
    "BedrockCreateProvisionedModelThroughputOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockCreateProvisionedModelThroughputOperator",
    "BedrockCreateKnowledgeBaseOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockCreateKnowledgeBaseOperator",
    "BedrockCreateDataSourceOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockCreateDataSourceOperator",
    "BedrockIngestDataOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockIngestDataOperator",
    "BedrockRaGOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockRaGOperator",
    "BedrockRetrieveOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockRetrieveOperator",
    "BedrockCreateGuardrailOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockCreateGuardrailOperator",
    "BedrockCreateGuardrailVersionOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockCreateGuardrailVersionOperator",
    "BedrockDeleteGuardrailOperator": "airflow.providers.amazon.aws.operators.bedrock.BedrockDeleteGuardrailOperator",
    "BedrockBaseSensor": "airflow.providers.amazon.aws.sensors.bedrock.BedrockBaseSensor",
    "BedrockCustomizeModelCompletedSensor": "airflow.providers.amazon.aws.sensors.bedrock.BedrockCustomizeModelCompletedSensor",
    "BedrockProvisionModelThroughputCompletedSensor": "airflow.providers.amazon.aws.sensors.bedrock.BedrockProvisionModelThroughputCompletedSensor",
    "BedrockKnowledgeBaseActiveSensor": "airflow.providers.amazon.aws.sensors.bedrock.BedrockKnowledgeBaseActiveSensor",
    "BedrockIngestionJobSensor": "airflow.providers.amazon.aws.sensors.bedrock.BedrockIngestionJobSensor",
    # ── Lambda ──
    "LambdaCreateFunctionOperator": "airflow.providers.amazon.aws.operators.lambda_function.LambdaCreateFunctionOperator",
    "LambdaInvokeFunctionOperator": "airflow.providers.amazon.aws.operators.lambda_function.LambdaInvokeFunctionOperator",
    "LambdaFunctionStateSensor": "airflow.providers.amazon.aws.sensors.lambda_function.LambdaFunctionStateSensor",
    # ── Step Functions ──
    "StepFunctionStartExecutionOperator": "airflow.providers.amazon.aws.operators.step_function.StepFunctionStartExecutionOperator",
    "StepFunctionGetExecutionOutputOperator": "airflow.providers.amazon.aws.operators.step_function.StepFunctionGetExecutionOutputOperator",
    "StepFunctionExecutionSensor": "airflow.providers.amazon.aws.sensors.step_function.StepFunctionExecutionSensor",
    # ── EMR ──
    "EmrAddStepsOperator": "airflow.providers.amazon.aws.operators.emr.EmrAddStepsOperator",
    "EmrStartNotebookExecutionOperator": "airflow.providers.amazon.aws.operators.emr.EmrStartNotebookExecutionOperator",
    "EmrStopNotebookExecutionOperator": "airflow.providers.amazon.aws.operators.emr.EmrStopNotebookExecutionOperator",
    "EmrEksCreateClusterOperator": "airflow.providers.amazon.aws.operators.emr.EmrEksCreateClusterOperator",
    "EmrContainerOperator": "airflow.providers.amazon.aws.operators.emr.EmrContainerOperator",
    "EmrCreateJobFlowOperator": "airflow.providers.amazon.aws.operators.emr.EmrCreateJobFlowOperator",
    "EmrModifyClusterOperator": "airflow.providers.amazon.aws.operators.emr.EmrModifyClusterOperator",
    "EmrTerminateJobFlowOperator": "airflow.providers.amazon.aws.operators.emr.EmrTerminateJobFlowOperator",
    "EmrServerlessCreateApplicationOperator": "airflow.providers.amazon.aws.operators.emr.EmrServerlessCreateApplicationOperator",
    "EmrServerlessStartJobOperator": "airflow.providers.amazon.aws.operators.emr.EmrServerlessStartJobOperator",
    "EmrServerlessStopApplicationOperator": "airflow.providers.amazon.aws.operators.emr.EmrServerlessStopApplicationOperator",
    "EmrServerlessDeleteApplicationOperator": "airflow.providers.amazon.aws.operators.emr.EmrServerlessDeleteApplicationOperator",
    "EmrBaseSensor": "airflow.providers.amazon.aws.sensors.emr.EmrBaseSensor",
    "EmrServerlessJobSensor": "airflow.providers.amazon.aws.sensors.emr.EmrServerlessJobSensor",
    "EmrServerlessApplicationSensor": "airflow.providers.amazon.aws.sensors.emr.EmrServerlessApplicationSensor",
    "EmrContainerSensor": "airflow.providers.amazon.aws.sensors.emr.EmrContainerSensor",
    "EmrNotebookExecutionSensor": "airflow.providers.amazon.aws.sensors.emr.EmrNotebookExecutionSensor",
    "EmrJobFlowSensor": "airflow.providers.amazon.aws.sensors.emr.EmrJobFlowSensor",
    "EmrStepSensor": "airflow.providers.amazon.aws.sensors.emr.EmrStepSensor",
    # ── Batch ──
    "BatchOperator": "airflow.providers.amazon.aws.operators.batch.BatchOperator",
    "BatchCreateComputeEnvironmentOperator": "airflow.providers.amazon.aws.operators.batch.BatchCreateComputeEnvironmentOperator",
    "BatchSensor": "airflow.providers.amazon.aws.sensors.batch.BatchSensor",
    "BatchComputeEnvironmentSensor": "airflow.providers.amazon.aws.sensors.batch.BatchComputeEnvironmentSensor",
    "BatchJobQueueSensor": "airflow.providers.amazon.aws.sensors.batch.BatchJobQueueSensor",
    # ── ECS ──
    "EcsBaseOperator": "airflow.providers.amazon.aws.operators.ecs.EcsBaseOperator",
    "EcsCreateClusterOperator": "airflow.providers.amazon.aws.operators.ecs.EcsCreateClusterOperator",
    "EcsDeleteClusterOperator": "airflow.providers.amazon.aws.operators.ecs.EcsDeleteClusterOperator",
    "EcsDeregisterTaskDefinitionOperator": "airflow.providers.amazon.aws.operators.ecs.EcsDeregisterTaskDefinitionOperator",
    "EcsRegisterTaskDefinitionOperator": "airflow.providers.amazon.aws.operators.ecs.EcsRegisterTaskDefinitionOperator",
    "EcsRunTaskOperator": "airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator",
    "EcsBaseSensor": "airflow.providers.amazon.aws.sensors.ecs.EcsBaseSensor",
    "EcsClusterStateSensor": "airflow.providers.amazon.aws.sensors.ecs.EcsClusterStateSensor",
    "EcsTaskDefinitionStateSensor": "airflow.providers.amazon.aws.sensors.ecs.EcsTaskDefinitionStateSensor",
    "EcsTaskStateSensor": "airflow.providers.amazon.aws.sensors.ecs.EcsTaskStateSensor",
    # ── EKS ──
    "EksCreateClusterOperator": "airflow.providers.amazon.aws.operators.eks.EksCreateClusterOperator",
    "EksCreateNodegroupOperator": "airflow.providers.amazon.aws.operators.eks.EksCreateNodegroupOperator",
    "EksCreateFargateProfileOperator": "airflow.providers.amazon.aws.operators.eks.EksCreateFargateProfileOperator",
    "EksDeleteClusterOperator": "airflow.providers.amazon.aws.operators.eks.EksDeleteClusterOperator",
    "EksDeleteNodegroupOperator": "airflow.providers.amazon.aws.operators.eks.EksDeleteNodegroupOperator",
    "EksDeleteFargateProfileOperator": "airflow.providers.amazon.aws.operators.eks.EksDeleteFargateProfileOperator",
    "EksPodOperator": "airflow.providers.amazon.aws.operators.eks.EksPodOperator",
    "EksBaseSensor": "airflow.providers.amazon.aws.sensors.eks.EksBaseSensor",
    "EksClusterStateSensor": "airflow.providers.amazon.aws.sensors.eks.EksClusterStateSensor",
    "EksFargateProfileStateSensor": "airflow.providers.amazon.aws.sensors.eks.EksFargateProfileStateSensor",
    "EksNodegroupStateSensor": "airflow.providers.amazon.aws.sensors.eks.EksNodegroupStateSensor",
    # ── CloudFormation ──
    "CloudFormationCreateStackOperator": "airflow.providers.amazon.aws.operators.cloud_formation.CloudFormationCreateStackOperator",
    "CloudFormationDeleteStackOperator": "airflow.providers.amazon.aws.operators.cloud_formation.CloudFormationDeleteStackOperator",
    "CloudFormationCreateStackSensor": "airflow.providers.amazon.aws.sensors.cloud_formation.CloudFormationCreateStackSensor",
    "CloudFormationDeleteStackSensor": "airflow.providers.amazon.aws.sensors.cloud_formation.CloudFormationDeleteStackSensor",
    # ── SageMaker ──
    "SageMakerNotebookOperator": "airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator",
    "SageMakerBaseOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerBaseOperator",
    "SageMakerProcessingOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator",
    "SageMakerEndpointConfigOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerEndpointConfigOperator",
    "SageMakerEndpointOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerEndpointOperator",
    "SageMakerTransformOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator",
    "SageMakerTuningOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerTuningOperator",
    "SageMakerModelOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerModelOperator",
    "SageMakerTrainingOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerTrainingOperator",
    "SageMakerDeleteModelOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerDeleteModelOperator",
    "SageMakerStartPipelineOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerStartPipelineOperator",
    "SageMakerStopPipelineOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerStopPipelineOperator",
    "SageMakerRegisterModelVersionOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerRegisterModelVersionOperator",
    "SageMakerAutoMLOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerAutoMLOperator",
    "SageMakerCreateExperimentOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerCreateExperimentOperator",
    "SageMakerCreateNotebookOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerCreateNotebookOperator",
    "SageMakerStopNotebookOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerStopNotebookOperator",
    "SageMakerDeleteNotebookOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerDeleteNotebookOperator",
    "SageMakerStartNoteBookOperator": "airflow.providers.amazon.aws.operators.sagemaker.SageMakerStartNoteBookOperator",
    "SageMakerBaseSensor": "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerBaseSensor",
    "SageMakerEndpointSensor": "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerEndpointSensor",
    "SageMakerTransformSensor": "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTransformSensor",
    "SageMakerTuningSensor": "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTuningSensor",
    "SageMakerTrainingSensor": "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTrainingSensor",
    "SageMakerPipelineSensor": "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerPipelineSensor",
    "SageMakerAutoMLSensor": "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerAutoMLSensor",
    "SageMakerProcessingSensor": "airflow.providers.amazon.aws.sensors.sagemaker.SageMakerProcessingSensor",
    "SageMakerNotebookSensor": "airflow.providers.amazon.aws.sensors.sagemaker_unified_studio.SageMakerNotebookSensor",
    # ── RDS ──
    "RdsBaseOperator": "airflow.providers.amazon.aws.operators.rds.RdsBaseOperator",
    "RdsCreateDbSnapshotOperator": "airflow.providers.amazon.aws.operators.rds.RdsCreateDbSnapshotOperator",
    "RdsCopyDbSnapshotOperator": "airflow.providers.amazon.aws.operators.rds.RdsCopyDbSnapshotOperator",
    "RdsDeleteDbSnapshotOperator": "airflow.providers.amazon.aws.operators.rds.RdsDeleteDbSnapshotOperator",
    "RdsStartExportTaskOperator": "airflow.providers.amazon.aws.operators.rds.RdsStartExportTaskOperator",
    "RdsCancelExportTaskOperator": "airflow.providers.amazon.aws.operators.rds.RdsCancelExportTaskOperator",
    "RdsCreateEventSubscriptionOperator": "airflow.providers.amazon.aws.operators.rds.RdsCreateEventSubscriptionOperator",
    "RdsDeleteEventSubscriptionOperator": "airflow.providers.amazon.aws.operators.rds.RdsDeleteEventSubscriptionOperator",
    "RdsCreateDbInstanceOperator": "airflow.providers.amazon.aws.operators.rds.RdsCreateDbInstanceOperator",
    "RdsDeleteDbInstanceOperator": "airflow.providers.amazon.aws.operators.rds.RdsDeleteDbInstanceOperator",
    "RdsStartDbOperator": "airflow.providers.amazon.aws.operators.rds.RdsStartDbOperator",
    "RdsStopDbOperator": "airflow.providers.amazon.aws.operators.rds.RdsStopDbOperator",
    "RdsBaseSensor": "airflow.providers.amazon.aws.sensors.rds.RdsBaseSensor",
    "RdsSnapshotExistenceSensor": "airflow.providers.amazon.aws.sensors.rds.RdsSnapshotExistenceSensor",
    "RdsExportTaskExistenceSensor": "airflow.providers.amazon.aws.sensors.rds.RdsExportTaskExistenceSensor",
    "RdsDbSensor": "airflow.providers.amazon.aws.sensors.rds.RdsDbSensor",
    # ── Redshift ──
    "RedshiftCreateClusterOperator": "airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftCreateClusterOperator",
    "RedshiftCreateClusterSnapshotOperator": "airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftCreateClusterSnapshotOperator",
    "RedshiftDeleteClusterSnapshotOperator": "airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftDeleteClusterSnapshotOperator",
    "RedshiftResumeClusterOperator": "airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftResumeClusterOperator",
    "RedshiftPauseClusterOperator": "airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftPauseClusterOperator",
    "RedshiftDeleteClusterOperator": "airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftDeleteClusterOperator",
    "RedshiftDataOperator": "airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator",
    "RedshiftClusterSensor": "airflow.providers.amazon.aws.sensors.redshift_cluster.RedshiftClusterSensor",
    # ── DMS ──
    "DmsCreateTaskOperator": "airflow.providers.amazon.aws.operators.dms.DmsCreateTaskOperator",
    "DmsDeleteTaskOperator": "airflow.providers.amazon.aws.operators.dms.DmsDeleteTaskOperator",
    "DmsDescribeTasksOperator": "airflow.providers.amazon.aws.operators.dms.DmsDescribeTasksOperator",
    "DmsStartTaskOperator": "airflow.providers.amazon.aws.operators.dms.DmsStartTaskOperator",
    "DmsStopTaskOperator": "airflow.providers.amazon.aws.operators.dms.DmsStopTaskOperator",
    "DmsTaskBaseSensor": "airflow.providers.amazon.aws.sensors.dms.DmsTaskBaseSensor",
    "DmsTaskCompletedSensor": "airflow.providers.amazon.aws.sensors.dms.DmsTaskCompletedSensor",
    # ── EC2 ──
    "EC2StartInstanceOperator": "airflow.providers.amazon.aws.operators.ec2.EC2StartInstanceOperator",
    "EC2StopInstanceOperator": "airflow.providers.amazon.aws.operators.ec2.EC2StopInstanceOperator",
    "EC2CreateInstanceOperator": "airflow.providers.amazon.aws.operators.ec2.EC2CreateInstanceOperator",
    "EC2TerminateInstanceOperator": "airflow.providers.amazon.aws.operators.ec2.EC2TerminateInstanceOperator",
    "EC2RebootInstanceOperator": "airflow.providers.amazon.aws.operators.ec2.EC2RebootInstanceOperator",
    "EC2HibernateInstanceOperator": "airflow.providers.amazon.aws.operators.ec2.EC2HibernateInstanceOperator",
    "EC2InstanceStateSensor": "airflow.providers.amazon.aws.sensors.ec2.EC2InstanceStateSensor",
    # ── SNS / SQS ──
    "SnsPublishOperator": "airflow.providers.amazon.aws.operators.sns.SnsPublishOperator",
    "SqsPublishOperator": "airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator",
    "SqsSensor": "airflow.providers.amazon.aws.sensors.sqs.SqsSensor",
    # ── EventBridge ──
    "EventBridgePutEventsOperator": "airflow.providers.amazon.aws.operators.eventbridge.EventBridgePutEventsOperator",
    "EventBridgePutRuleOperator": "airflow.providers.amazon.aws.operators.eventbridge.EventBridgePutRuleOperator",
    "EventBridgeEnableRuleOperator": "airflow.providers.amazon.aws.operators.eventbridge.EventBridgeEnableRuleOperator",
    "EventBridgeDisableRuleOperator": "airflow.providers.amazon.aws.operators.eventbridge.EventBridgeDisableRuleOperator",
    # ── Comprehend ──
    "ComprehendBaseOperator": "airflow.providers.amazon.aws.operators.comprehend.ComprehendBaseOperator",
    "ComprehendStartPiiEntitiesDetectionJobOperator": "airflow.providers.amazon.aws.operators.comprehend.ComprehendStartPiiEntitiesDetectionJobOperator",
    "ComprehendCreateDocumentClassifierOperator": "airflow.providers.amazon.aws.operators.comprehend.ComprehendCreateDocumentClassifierOperator",
    "ComprehendBaseSensor": "airflow.providers.amazon.aws.sensors.comprehend.ComprehendBaseSensor",
    "ComprehendStartPiiEntitiesDetectionJobCompletedSensor": "airflow.providers.amazon.aws.sensors.comprehend.ComprehendStartPiiEntitiesDetectionJobCompletedSensor",
    "ComprehendCreateDocumentClassifierCompletedSensor": "airflow.providers.amazon.aws.sensors.comprehend.ComprehendCreateDocumentClassifierCompletedSensor",
    # ── Kinesis Analytics ──
    "KinesisAnalyticsV2CreateApplicationOperator": "airflow.providers.amazon.aws.operators.kinesis_analytics.KinesisAnalyticsV2CreateApplicationOperator",
    "KinesisAnalyticsV2StartApplicationOperator": "airflow.providers.amazon.aws.operators.kinesis_analytics.KinesisAnalyticsV2StartApplicationOperator",
    "KinesisAnalyticsV2StopApplicationOperator": "airflow.providers.amazon.aws.operators.kinesis_analytics.KinesisAnalyticsV2StopApplicationOperator",
    "KinesisAnalyticsV2BaseSensor": "airflow.providers.amazon.aws.sensors.kinesis_analytics.KinesisAnalyticsV2BaseSensor",
    "KinesisAnalyticsV2StartApplicationCompletedSensor": "airflow.providers.amazon.aws.sensors.kinesis_analytics.KinesisAnalyticsV2StartApplicationCompletedSensor",
    "KinesisAnalyticsV2StopApplicationCompletedSensor": "airflow.providers.amazon.aws.sensors.kinesis_analytics.KinesisAnalyticsV2StopApplicationCompletedSensor",
    # ── Neptune ──
    "NeptuneStartDbClusterOperator": "airflow.providers.amazon.aws.operators.neptune.NeptuneStartDbClusterOperator",
    "NeptuneStopDbClusterOperator": "airflow.providers.amazon.aws.operators.neptune.NeptuneStopDbClusterOperator",
    # ── Glacier ──
    "GlacierCreateJobOperator": "airflow.providers.amazon.aws.operators.glacier.GlacierCreateJobOperator",
    "GlacierUploadArchiveOperator": "airflow.providers.amazon.aws.operators.glacier.GlacierUploadArchiveOperator",
    "GlacierJobOperationSensor": "airflow.providers.amazon.aws.sensors.glacier.GlacierJobOperationSensor",
    # ── DataSync ──
    "DataSyncOperator": "airflow.providers.amazon.aws.operators.datasync.DataSyncOperator",
    # ── AppFlow ──
    "AppflowBaseOperator": "airflow.providers.amazon.aws.operators.appflow.AppflowBaseOperator",
    "AppflowRunOperator": "airflow.providers.amazon.aws.operators.appflow.AppflowRunOperator",
    "AppflowRunFullOperator": "airflow.providers.amazon.aws.operators.appflow.AppflowRunFullOperator",
    "AppflowRunBeforeOperator": "airflow.providers.amazon.aws.operators.appflow.AppflowRunBeforeOperator",
    "AppflowRunAfterOperator": "airflow.providers.amazon.aws.operators.appflow.AppflowRunAfterOperator",
    "AppflowRunDailyOperator": "airflow.providers.amazon.aws.operators.appflow.AppflowRunDailyOperator",
    "AppflowRecordsShortCircuitOperator": "airflow.providers.amazon.aws.operators.appflow.AppflowRecordsShortCircuitOperator",
    # ── QuickSight ──
    "QuickSightCreateIngestionOperator": "airflow.providers.amazon.aws.operators.quicksight.QuickSightCreateIngestionOperator",
    "QuickSightSensor": "airflow.providers.amazon.aws.sensors.quicksight.QuickSightSensor",
    # ── DynamoDB ──
    "DynamoDBValueSensor": "airflow.providers.amazon.aws.sensors.dynamodb.DynamoDBValueSensor",
    # ── OpenSearch Serverless ──
    "OpenSearchServerlessCollectionActiveSensor": "airflow.providers.amazon.aws.sensors.opensearch_serverless.OpenSearchServerlessCollectionActiveSensor",
    # ── Base sensors ──
    "AwsBaseSensor": "airflow.providers.amazon.aws.sensors.base_aws.AwsBaseSensor",
}

# ── Alternate FQNs the service also accepts (verified) ──
# Airflow 3 moved core operators into the "standard" provider but keeps the
# legacy import paths working. Both forms are accepted by MWAA Serverless.
ALT_OPERATOR_FQNS = {
    "airflow.providers.standard.operators.empty.EmptyOperator": "EmptyOperator",
    "airflow.operators.python.PythonOperator": "PythonOperator",
    "airflow.operators.bash.BashOperator": "BashOperator",
}

# ── Abstract base classes: in the allowlist but NOT usable as a task operator ──
ABSTRACT_OPERATORS = {
    "AwsBaseSensor", "EcsBaseOperator", "EcsBaseSensor", "EksBaseSensor",
    "SageMakerBaseOperator", "SageMakerBaseSensor", "BedrockBaseSensor",
    "EmrBaseSensor", "AppflowBaseOperator", "ComprehendBaseOperator",
    "ComprehendBaseSensor", "RdsBaseOperator", "RdsBaseSensor",
    "DmsTaskBaseSensor", "KinesisAnalyticsV2BaseSensor", "BatchOperatorBase",
}

# ── Operators that require code to be uploaded via the CreateWorkflow `Code` parameter ──
CODE_OPERATORS = {"PythonOperator", "BashOperator"}

# ══════════════════════════════════════════════════════════════════════════
#  SENSOR COST CONTROL
# ══════════════════════════════════════════════════════════════════════════
# How a sensor waits matters, because MWAA Serverless bills for the time a task
# occupies a worker. Two Airflow features look like they address this and currently
# do not apply here:
#
#   mode: reschedule  Accepted and enum-validated at create time, but not supported
#                     end to end — the wait does not resume. Leave sensors in the
#                     default poke mode.
#   deferrable: true  Accepted and then ignored; CreateWorkflow reports it under
#                     Warnings: ['ignored attributes: deferrable'].
#
# Both are re-checked periodically against the live service; see SENSOR_MODE_SUPPORT
# for the single place to update when that changes. Until then the lever is to wait
# less rather than to wait differently: fewer sensors, bounded timeouts, and letting
# an operator's own wait_for_completion block instead of adding a second task.
SENSOR_MODES = ("poke", "reschedule")

# Whether reschedule mode can be used. Flip this to True (and drop the message) once
# the service supports it, and the validator, builder and repair paths follow.
RESCHEDULE_MODE_SUPPORTED = False

RESCHEDULE_MODE_UNSUPPORTED = (
    "mode: reschedule is accepted at create time but is not currently supported end to "
    "end on MWAA Serverless — the wait does not resume, so the task does not complete. "
    "Use the default poke mode and bound it with a timeout."
)

# Sensor arguments the service accepts. `mode` is deliberately absent from the
# defaults applied by the builder — see RESCHEDULE_MODE_SUPPORTED.
SENSOR_COST_PARAMS = {
    "poke_interval": "Seconds between checks. Lower means more API calls; it does not "
                     "reduce cost, because the worker is held either way.",
    "timeout": "Seconds (or a __type__ timedelta mapping) before the sensor gives up. "
               "Airflow's default is 7 days, which on a billed platform is a real hazard.",
    "soft_fail": "true to mark the task SKIPPED instead of FAILED on timeout.",
    "exponential_backoff": "true to grow the interval between checks.",
    "max_wait": "Upper bound on the interval when exponential_backoff is on.",
}

# Applied to sensors by the builder. Only a bounded wait — nothing that changes how
# the task is scheduled, and nothing the caller did not ask for beyond safety.
SENSOR_SAFETY_DEFAULTS = {"timeout": 3600}

# Operators whose own blocking wait would otherwise tempt an author into adding a
# second sensor task. Maps operator -> the sensor that would pair with it.
LONG_WAIT_OPERATOR_PAIRS = {
    "GlueJobOperator": "GlueJobSensor",
    "GlueCrawlerOperator": "GlueCrawlerSensor",
    "GlueDataQualityRuleSetEvaluationRunOperator": "GlueDataQualityRuleSetEvaluationRunSensor",
    "AthenaOperator": "AthenaSensor",
    "EmrCreateJobFlowOperator": "EmrJobFlowSensor",
    "EmrAddStepsOperator": "EmrStepSensor",
    "EmrServerlessStartJobOperator": "EmrServerlessJobSensor",
    "EmrContainerOperator": "EmrContainerSensor",
    "BatchOperator": "BatchSensor",
    "StepFunctionStartExecutionOperator": "StepFunctionExecutionSensor",
    "SageMakerTrainingOperator": "SageMakerTrainingSensor",
    "SageMakerTransformOperator": "SageMakerTransformSensor",
    "SageMakerTuningOperator": "SageMakerTuningSensor",
    "SageMakerEndpointOperator": "SageMakerEndpointSensor",
    "RdsCreateDbInstanceOperator": "RdsDbSensor",
    "RedshiftCreateClusterOperator": "RedshiftClusterSensor",
    "BedrockCustomizeModelOperator": "BedrockCustomizeModelCompletedSensor",
    "BedrockIngestDataOperator": "BedrockIngestionJobSensor",
}


def is_sensor(operator: str) -> bool:
    """True if the operator (short name or FQN) is a sensor.

    Sensors are the only operators that accept `mode`, and the only ones where a
    bounded `timeout` is a meaningful safety default.
    """
    if not operator:
        return False
    name = operator.rsplit(".", 1)[-1]
    return name.endswith("Sensor")


# ── Semantically required parameters per operator ──
# MWAA Serverless only rejects a task at create time when the operator's Python
# __init__ has a required positional arg. Many operators default their key
# argument to None and then fail at RUN time — which is the single largest
# source of "the DAG deployed but every task failed". These are validated
# client-side so the problem surfaces before deployment.
OPERATOR_REQUIRED_PARAMS = {
    # Core
    "PythonOperator": ["python_callable"],
    "BashOperator": ["bash_command"],
    # S3
    "S3CreateBucketOperator": ["bucket_name"],
    "S3DeleteBucketOperator": ["bucket_name"],
    "S3CreateObjectOperator": ["s3_bucket", "s3_key", "data"],
    "S3DeleteObjectsOperator": ["bucket"],
    "S3ListOperator": ["bucket"],
    "S3ListPrefixesOperator": ["bucket", "prefix", "delimiter"],
    "S3CopyObjectOperator": ["source_bucket_key", "dest_bucket_key"],
    "S3PutBucketTaggingOperator": ["bucket_name"],
    "S3GetBucketTaggingOperator": ["bucket_name"],
    "S3DeleteBucketTaggingOperator": ["bucket_name"],
    "S3KeySensor": ["bucket_key"],
    "S3KeysUnchangedSensor": ["bucket_name", "prefix"],
    # Glue
    "GlueJobOperator": ["job_name"],
    "GlueJobSensor": ["job_name", "run_id"],
    "GlueCrawlerOperator": ["config"],
    "GlueCrawlerSensor": ["crawler_name"],
    "GlueDataBrewStartJobOperator": ["job_name"],
    "GlueDataQualityOperator": ["name", "ruleset"],
    "GlueCatalogPartitionSensor": ["table_name"],
    "GlueCatalogCreateDatabaseOperator": ["database_input"],
    "GlueCatalogCreateTableOperator": ["table_input"],
    "GlueCatalogDeleteDatabaseOperator": ["database_name"],
    "GlueCatalogDeleteTableOperator": ["table_name"],
    # Athena
    "AthenaOperator": ["query", "database", "output_location"],
    "AthenaSensor": ["query_execution_id"],
    # Lambda
    "LambdaInvokeFunctionOperator": ["function_name"],
    "LambdaCreateFunctionOperator": ["function_name", "runtime", "role", "handler", "code"],
    "LambdaFunctionStateSensor": ["function_name"],
    # Step Functions
    "StepFunctionStartExecutionOperator": ["state_machine_arn"],
    "StepFunctionExecutionSensor": ["execution_arn"],
    "StepFunctionGetExecutionOutputOperator": ["execution_arn"],
    # Redshift
    "RedshiftDataOperator": ["sql", "database"],
    "RedshiftCreateClusterOperator": ["cluster_identifier", "node_type", "master_username", "master_user_password"],
    "RedshiftDeleteClusterOperator": ["cluster_identifier"],
    "RedshiftClusterSensor": ["cluster_identifier", "target_status"],
    # EMR Serverless / EMR
    "EmrServerlessCreateApplicationOperator": ["release_label", "job_type"],
    "EmrServerlessStartJobOperator": ["application_id", "execution_role_arn", "job_driver"],
    "EmrServerlessJobSensor": ["application_id", "job_run_id"],
    "EmrServerlessDeleteApplicationOperator": ["application_id"],
    "EmrServerlessStopApplicationOperator": ["application_id"],
    "EmrAddStepsOperator": ["steps"],
    "EmrCreateJobFlowOperator": ["job_flow_overrides"],
    "EmrTerminateJobFlowOperator": ["job_flow_id"],
    "EmrStepSensor": ["job_flow_id", "step_id"],
    "EmrJobFlowSensor": ["job_flow_id"],
    "EmrContainerOperator": ["virtual_cluster_id", "execution_role_arn", "release_label", "job_driver"],
    # Batch / ECS / EKS
    "BatchOperator": ["job_name", "job_definition", "job_queue"],
    "BatchSensor": ["job_id"],
    "EcsRunTaskOperator": ["task_definition", "cluster"],
    "EcsRegisterTaskDefinitionOperator": ["family", "container_definitions"],
    "EcsCreateClusterOperator": ["cluster_name"],
    "EcsDeleteClusterOperator": ["cluster_name"],
    "EksCreateClusterOperator": ["cluster_name", "cluster_role_arn", "resources_vpc_config"],
    "EksDeleteClusterOperator": ["cluster_name"],
    "EksPodOperator": ["cluster_name", "pod_name", "image"],
    # Messaging
    "SnsPublishOperator": ["target_arn", "message"],
    "SqsPublishOperator": ["sqs_queue", "message_content"],
    "SqsSensor": ["sqs_queue"],
    "EventBridgePutEventsOperator": ["entries"],
    # CloudFormation
    "CloudFormationCreateStackOperator": ["stack_name", "cloudformation_parameters"],
    "CloudFormationDeleteStackOperator": ["stack_name"],
    "CloudFormationCreateStackSensor": ["stack_name"],
    "CloudFormationDeleteStackSensor": ["stack_name"],
    # Bedrock
    "BedrockInvokeModelOperator": ["model_id", "input_data"],
    "BedrockCustomizeModelOperator": ["job_name", "custom_model_name", "role_arn", "base_model_id"],
    "BedrockRetrieveOperator": ["retrieval_query", "knowledge_base_id"],
    # Other
    "DynamoDBValueSensor": ["table_name", "partition_key_name", "partition_key_value", "attribute_name", "attribute_value"],
    "QuickSightCreateIngestionOperator": ["data_set_id", "ingestion_id"],
    "SageMakerTrainingOperator": ["config"],
    "SageMakerProcessingOperator": ["config"],
    "SageMakerTransformOperator": ["config"],
    "SageMakerStartPipelineOperator": ["pipeline_name"],
    "RdsCreateDbInstanceOperator": ["db_instance_identifier", "db_instance_class", "engine"],
    "RdsDeleteDbInstanceOperator": ["db_instance_identifier"],
    "RdsCreateDbSnapshotOperator": ["db_type", "db_identifier", "db_snapshot_identifier"],
    "NeptuneStartDbClusterOperator": ["db_cluster_id"],
    "NeptuneStopDbClusterOperator": ["db_cluster_id"],
    "EC2CreateInstanceOperator": ["image_id"],
    "EC2StartInstanceOperator": ["instance_id"],
    "EC2StopInstanceOperator": ["instance_id"],
    "EC2TerminateInstanceOperator": ["instance_ids"],
    "GlacierCreateJobOperator": ["vault_name"],
    "GlacierUploadArchiveOperator": ["vault_name", "body"],
    "AppflowRunOperator": ["flow_name"],
    "DmsCreateTaskOperator": ["replication_task_id", "source_endpoint_arn", "target_endpoint_arn",
                              "replication_instance_arn", "table_mappings"],
    "DmsStartTaskOperator": ["replication_task_arn"],
    "DmsStopTaskOperator": ["replication_task_arn"],
    "KinesisAnalyticsV2CreateApplicationOperator": ["application_name", "runtime_environment",
                                                    "service_execution_role", "create_application_kwargs"],
    "KinesisAnalyticsV2StartApplicationOperator": ["application_name"],
    "ComprehendStartPiiEntitiesDetectionJobOperator": ["input_data_config", "output_data_config",
                                                       "mode", "data_access_role_arn", "language_code"],
    "S3TablesCreateTableBucketOperator": ["name"],
    "S3TablesCreateNamespaceOperator": ["namespace", "table_bucket_arn"],
    "S3TablesCreateTableOperator": ["namespace", "table_bucket_arn", "name", "format"],
    "S3VectorsCreateVectorBucketOperator": ["vector_bucket_name"],
    "S3VectorsCreateIndexOperator": ["vector_bucket_name", "index_name", "dimension",
                                     "data_type", "distance_metric"],
    "MwaaServerlessStartWorkflowRunOperator": ["workflow_arn"],
}

# ── What each operator pushes to XCom ──
# Downstream tasks read these with {{ ti.xcom_pull(task_ids='<upstream>') }}.
# This is how parameters are passed between tasks in MWAA Serverless — there is
# no PythonOperator-free alternative for passing derived values.
OPERATOR_XCOM_RETURNS = {
    "GlueJobOperator": "str — the Glue job RUN ID. Feed to GlueJobSensor.run_id.",
    "GlueCrawlerOperator": "str — the crawler name.",
    "AthenaOperator": "str — the Athena query execution ID. Feed to AthenaSensor.query_execution_id.",
    "S3ListOperator": "list[str] — matching object keys.",
    "S3ListPrefixesOperator": "list[str] — matching prefixes.",
    "S3GetBucketTaggingOperator": "list[dict] — bucket tag set.",
    "LambdaInvokeFunctionOperator": "str — the function response payload (str). Use `| from_json` style parsing in Python code, not Jinja.",
    "StepFunctionStartExecutionOperator": "str — the execution ARN. Feed to StepFunctionExecutionSensor.execution_arn.",
    "StepFunctionGetExecutionOutputOperator": "dict — the state machine output.",
    "EmrServerlessCreateApplicationOperator": "str — the EMR Serverless application ID.",
    "EmrServerlessStartJobOperator": "str — the job run ID.",
    "EmrCreateJobFlowOperator": "str — the EMR job flow (cluster) ID.",
    "EmrAddStepsOperator": "list[str] — the added step IDs.",
    "BatchOperator": "str — the Batch job ID.",
    "EcsRunTaskOperator": "str — last log message, or the task ARN depending on config.",
    "EcsRegisterTaskDefinitionOperator": "str — the task definition ARN.",
    "RedshiftDataOperator": "str | list — statement ID, or rows when return_sql_result=True.",
    "BedrockInvokeModelOperator": "dict — the model invocation response body.",
    "PythonOperator": "Whatever the callable returns (must be JSON-serialisable, <= 100 KB).",
    "BashOperator": "str — the last line of stdout.",
    "SqsSensor": "list[dict] — the received messages.",
    "CloudFormationCreateStackOperator": "None — CloudFormation stack outputs are NOT returned via XCom. "
                                         "Do not try to read stack Outputs from XCom; pass known values as params instead.",
}

# Reverse lookup: FQN -> short name
FQN_TO_SHORT = {v: k for k, v in SUPPORTED_OPERATORS.items()}
FQN_TO_SHORT.update(ALT_OPERATOR_FQNS)
_FQN_TO_SHORT = FQN_TO_SHORT  # backwards-compatible alias

# Set of all operator values the SERVICE accepts (FQNs only).
ALLOWED_OPERATOR_FQNS = set(SUPPORTED_OPERATORS.values()) | set(ALT_OPERATOR_FQNS.keys())

# Every recognised spelling, including short names. Short names are recognised by
# the tooling (so it can auto-correct them) but are NOT valid in deployed YAML.
ALLOWED_OPERATOR_VALUES = set(SUPPORTED_OPERATORS.keys()) | ALLOWED_OPERATOR_FQNS


def resolve_operator_fqn(operator: str):
    """Return (fqn, short_name, was_short_name) for an operator string.

    Returns (None, None, False) when the operator is not recognised at all.
    """
    if not isinstance(operator, str) or not operator:
        return None, None, False
    if operator in ALLOWED_OPERATOR_FQNS:
        return operator, FQN_TO_SHORT.get(operator, operator.rsplit(".", 1)[-1]), False
    if operator in SUPPORTED_OPERATORS:
        return SUPPORTED_OPERATORS[operator], operator, True
    return None, None, False
