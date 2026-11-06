// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_temporal/otel_temporal.dart';

/// Traces Temporal `StartWorkflowExecution` calls with OpenTelemetry.
///
/// `otel_temporal` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Temporal `StartWorkflowExecution` call in
/// [tracedTemporalStartWorkflow]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'temporal-example');

  // Wraps a Temporal StartWorkflowExecution call in a CLIENT span.
  final temporalStartWorkflowId = await tracedTemporalStartWorkflow<String>(
    workflowType: 'standard',
    workflowId: 'workflow_8f3a2c',
    taskQueue: 'orders',
    namespace: 'example',
    invoke: () async {
      // Make the Temporal `StartWorkflowExecution` call here, then report what it returned.
      const id = 'temporal_8f3a2c';
      return const TemporalStartResult(value: id, runId: 'run_8f3a2c');
    },
  );
  OTelLog.info('tracedTemporalStartWorkflow returned $temporalStartWorkflowId');

  // Wraps a Temporal signal/query/cancel call in a CLIENT span.
  final temporalSignalId = await tracedTemporalSignal<String>(
    operation: 'create',
    workflowId: 'workflow_8f3a2c',
    signalName: 'example',
    namespace: 'example',
    invoke: () async {
      // Make the Temporal `StartWorkflowExecution` call here, then report what it returned.
      return 'temporal_8f3a2c';
    },
  );
  OTelLog.info('tracedTemporalSignal create returned $temporalSignalId');

  // Wraps the body of a single Activity invocation in an INTERNAL span so each activity attempt appears as a child of the workflow.
  final temporalActivityId = await tracedTemporalActivity<String>(
    activityType: 'standard',
    attempt: 1,
    taskQueue: 'orders',
    invoke: () async {
      // Make the Temporal `StartWorkflowExecution` call here, then report what it returned.
      return 'temporal_8f3a2c';
    },
  );
  OTelLog.info('tracedTemporalActivity returned $temporalActivityId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutTemporalInstrumentationAsync.
  await runWithoutTemporalInstrumentationAsync(
    () => tracedTemporalStartWorkflow<String>(
      workflowType: 'standard',
      workflowId: 'workflow_8f3a2c',
      taskQueue: 'orders',
      namespace: 'example',
      invoke: () async {
        const id = 'temporal_8f3a2c';
        return const TemporalStartResult(value: id, runId: 'run_8f3a2c');
      },
    ),
  );

  await OTel.shutdown();
}
