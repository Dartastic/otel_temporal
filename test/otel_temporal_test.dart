// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_temporal/otel_temporal.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_temporal-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('StartWorkflow records CLIENT span', () async {
    await tracedTemporalStartWorkflow<String>(
      workflowType: 'OrderWorkflow',
      workflowId: 'order-1',
      taskQueue: 'orders-queue',
      namespace: 'default',
      invoke: () async => const TemporalStartResult(
        value: 'ok',
        runId: 'run-1',
      ),
    );
    final a = attrsOf(spans.findSpanByName('temporal start OrderWorkflow')!);
    expect(a['rpc.system'], 'temporal');
    expect(a['rpc.method'], 'StartWorkflowExecution');
    expect(a['temporal.workflow.type'], 'OrderWorkflow');
    expect(a['temporal.workflow.id'], 'order-1');
    expect(a['temporal.task_queue'], 'orders-queue');
    expect(a['temporal.namespace'], 'default');
    expect(a['temporal.workflow.run_id'], 'run-1');
  });

  test('signal records CLIENT span', () async {
    await tracedTemporalSignal<String>(
      operation: 'signal',
      workflowId: 'order-1',
      signalName: 'cancel',
      invoke: () async => 'ok',
    );
    final a = attrsOf(spans.findSpanByName('temporal signal order-1')!);
    expect(a['temporal.operation'], 'signal');
    expect(a['temporal.signal.name'], 'cancel');
  });

  test('activity records INTERNAL span', () async {
    await tracedTemporalActivity<String>(
      activityType: 'ChargeCard',
      attempt: 1,
      taskQueue: 'orders-queue',
      invoke: () async => 'ok',
    );
    final span = spans.findSpanByName('temporal activity ChargeCard');
    expect(span!.kind, SpanKind.internal);
    expect(attrsOf(span)['temporal.activity.attempt'], 1);
  });

  test('suppression skips spans', () async {
    await runWithoutTemporalInstrumentationAsync(() async {
      await tracedTemporalStartWorkflow<String>(
        workflowType: 't',
        workflowId: 'id',
        taskQueue: 'q',
        invoke: () async => const TemporalStartResult(value: 'v'),
      );
    });
    expect(spans.findSpansStartingWith('temporal '), isEmpty);
  });
}
