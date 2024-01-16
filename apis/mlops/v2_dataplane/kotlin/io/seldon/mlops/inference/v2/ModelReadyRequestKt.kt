/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed BY
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

//Generated by the protocol buffer compiler. DO NOT EDIT!
// source: v2_dataplane.proto

package io.seldon.mlops.inference.v2;

@kotlin.jvm.JvmSynthetic
public inline fun modelReadyRequest(block: io.seldon.mlops.inference.v2.ModelReadyRequestKt.Dsl.() -> kotlin.Unit): io.seldon.mlops.inference.v2.V2Dataplane.ModelReadyRequest =
  io.seldon.mlops.inference.v2.ModelReadyRequestKt.Dsl._create(io.seldon.mlops.inference.v2.V2Dataplane.ModelReadyRequest.newBuilder()).apply { block() }._build()
public object ModelReadyRequestKt {
  @kotlin.OptIn(com.google.protobuf.kotlin.OnlyForUseByGeneratedProtoCode::class)
  @com.google.protobuf.kotlin.ProtoDslMarker
  public class Dsl private constructor(
    private val _builder: io.seldon.mlops.inference.v2.V2Dataplane.ModelReadyRequest.Builder
  ) {
    public companion object {
      @kotlin.jvm.JvmSynthetic
      @kotlin.PublishedApi
      internal fun _create(builder: io.seldon.mlops.inference.v2.V2Dataplane.ModelReadyRequest.Builder): Dsl = Dsl(builder)
    }

    @kotlin.jvm.JvmSynthetic
    @kotlin.PublishedApi
    internal fun _build(): io.seldon.mlops.inference.v2.V2Dataplane.ModelReadyRequest = _builder.build()

    /**
     * <pre>
     * The name of the model to check for readiness.
     * </pre>
     *
     * <code>string name = 1;</code>
     */
    public var name: kotlin.String
      @JvmName("getName")
      get() = _builder.getName()
      @JvmName("setName")
      set(value) {
        _builder.setName(value)
      }
    /**
     * <pre>
     * The name of the model to check for readiness.
     * </pre>
     *
     * <code>string name = 1;</code>
     */
    public fun clearName() {
      _builder.clearName()
    }

    /**
     * <pre>
     * The version of the model to check for readiness. If not given the
     * server will choose a version based on the model and internal policy.
     * </pre>
     *
     * <code>string version = 2;</code>
     */
    public var version: kotlin.String
      @JvmName("getVersion")
      get() = _builder.getVersion()
      @JvmName("setVersion")
      set(value) {
        _builder.setVersion(value)
      }
    /**
     * <pre>
     * The version of the model to check for readiness. If not given the
     * server will choose a version based on the model and internal policy.
     * </pre>
     *
     * <code>string version = 2;</code>
     */
    public fun clearVersion() {
      _builder.clearVersion()
    }
  }
}
@kotlin.jvm.JvmSynthetic
public inline fun io.seldon.mlops.inference.v2.V2Dataplane.ModelReadyRequest.copy(block: io.seldon.mlops.inference.v2.ModelReadyRequestKt.Dsl.() -> kotlin.Unit): io.seldon.mlops.inference.v2.V2Dataplane.ModelReadyRequest =
  io.seldon.mlops.inference.v2.ModelReadyRequestKt.Dsl._create(this.toBuilder()).apply { block() }._build()
