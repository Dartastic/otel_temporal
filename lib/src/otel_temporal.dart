// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry_api/dartastic_opentelemetry_api.dart';

import 'otel_temporal_suppression.dart';

const _tracerName = 'dartastic_otel_libs_temporal';

APITracer _tracer() => OTelAPI.tracerProvider().getTracer(_tracerName);

/// Pair returned from `tracedTemporalStartWorkflow` callback.
class TemporalStartResult<T> {
  /// Creates a result.
  const TemporalStartResult({required this.value, this.runId});

  /// The value returned to the caller.
  final T value;
  final String? runId;
}

/// Wraps a Temporal `StartWorkflowExecution` call in a CLIENT span.
Future<T> tracedTemporalStartWorkflow<T>({
  required String workflowType,
  required String workflowId,
  required String taskQueue,
  required Future<TemporalStartResult<T>> Function() invoke,
  String? namespace,
}) async {
  if (temporalInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final m = <String, Object>{
    'temporal.workflow.type': workflowType,
    'temporal.workflow.id': workflowId,
    'temporal.task_queue': taskQueue,
    'rpc.system': 'temporal',
    'rpc.method': 'StartWorkflowExecution',
  };
  if (namespace != null) m['temporal.namespace'] = namespace;

  final span = _tracer().startSpan(
    'temporal start $workflowType',
    kind: SpanKind.client,
    attributes: OTelAPI.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.runId != null) {
      span.addAttributes(OTelAPI.attributes([
        OTelAPI.attributeString('temporal.workflow.run_id', r.runId!),
      ]));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(OTelAPI.attributes([
      OTelAPI.attributeString(
        ErrorAttributes.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps a Temporal signal/query/cancel call in a CLIENT span.
Future<T> tracedTemporalSignal<T>({
  required String operation,
  required String workflowId,
  required Future<T> Function() invoke,
  String? signalName,
  String? namespace,
}) async {
  if (temporalInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'rpc.system': 'temporal',
    'temporal.workflow.id': workflowId,
    'temporal.operation': operation,
  };
  if (signalName != null) m['temporal.signal.name'] = signalName;
  if (namespace != null) m['temporal.namespace'] = namespace;

  final span = _tracer().startSpan(
    'temporal $operation $workflowId',
    kind: SpanKind.client,
    attributes: OTelAPI.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTelAPI.attributes([
      OTelAPI.attributeString(
        ErrorAttributes.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps the body of a single Activity invocation in an INTERNAL
/// span so each activity attempt appears as a child of the workflow.
Future<T> tracedTemporalActivity<T>({
  required String activityType,
  required Future<T> Function() invoke,
  int? attempt,
  String? taskQueue,
}) async {
  if (temporalInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'temporal.activity.type': activityType,
  };
  if (attempt != null) m['temporal.activity.attempt'] = attempt;
  if (taskQueue != null) m['temporal.task_queue'] = taskQueue;

  final span = _tracer().startSpan(
    'temporal activity $activityType',
    kind: SpanKind.internal,
    attributes: OTelAPI.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTelAPI.attributes([
      OTelAPI.attributeString(
        ErrorAttributes.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
