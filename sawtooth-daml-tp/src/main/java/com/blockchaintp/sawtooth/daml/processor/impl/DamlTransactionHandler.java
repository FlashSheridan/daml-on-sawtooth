/*
 * Copyright 2019 Blockchain Technology Partners Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 * ------------------------------------------------------------------------------
 */
package com.blockchaintp.sawtooth.daml.processor.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import com.blockchaintp.sawtooth.daml.processor.LedgerState;
import com.blockchaintp.sawtooth.daml.protobuf.SawtoothDamlOperation;
import com.blockchaintp.sawtooth.daml.util.Namespace;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.caching.Cache;
import com.daml.metrics.Metrics;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.daml.ledger.validator.SubmissionValidator;
import com.daml.ledger.validator.ValidationFailed;
import com.daml.lf.data.Time.Timestamp;
import com.daml.lf.engine.Engine;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import sawtooth.sdk.processor.Context;
import sawtooth.sdk.processor.TransactionHandler;
import sawtooth.sdk.processor.exceptions.InternalError;
import sawtooth.sdk.processor.exceptions.InvalidTransactionException;
import sawtooth.sdk.protobuf.TpProcessRequest;
import sawtooth.sdk.protobuf.TransactionHeader;
import scala.compat.java8.FutureConverters;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.util.Either;

/**
 * A TransactionHandler implementation which handles DAML.
 *
 * @author scealiontach
 */
public final class DamlTransactionHandler implements TransactionHandler {

  private static final Logger LOGGER = Logger.getLogger(DamlTransactionHandler.class.getName());

  private final String familyName;
  private final String namespace;

  private final String version;

  private Metrics metricsRegistry;

  private Engine engine;

  /**
   * Constructs a TransactionHandler for DAML Transactions.
   *
   */
  public DamlTransactionHandler() {
    this.namespace = Namespace.getNameSpace();
    this.version = Namespace.DAML_FAMILY_VERSION_1_0;
    this.familyName = Namespace.DAML_FAMILY_NAME;
    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    this.metricsRegistry = new Metrics(SharedMetricRegistries.getOrCreate(hostname));
    this.engine = new Engine();
  }

  @Override
  public void apply(final TpProcessRequest tpProcessRequest, final Context state)
      throws InvalidTransactionException, InternalError {
    LOGGER.info(String.format("Processing transaction %s", tpProcessRequest.getSignature()));
    basicRequestChecks(tpProcessRequest);
    final LedgerState<String> ledgerState = new DamlLedgerState(state);
    try {
      SawtoothDamlOperation operation =
          SawtoothDamlOperation.parseFrom(tpProcessRequest.getPayload());
      switch (operation.getVersion()) {
        case ONE:
        case TWO:
          final ByteString envelopeBs = operation.getEnvelope();
          final ByteString logEntryIdBs = operation.getLogEntryId();
          final DamlLogEntryId logEntryId = DamlLogEntryId.parseFrom(logEntryIdBs);
          final String correlationId = operation.getCorrelationId();
          final String participantId = operation.getSubmittingParticipant();
          pocessTransaction(ledgerState, participantId, correlationId, logEntryId, envelopeBs);
          break;
        case UNKNOWN:
        case UNRECOGNIZED:
        default:
          throw new InvalidTransactionException(
              String.format("Unknown SawtoothDamlOperation %s", operation.getVersion().name()));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new InvalidTransactionException(e.getMessage());
    }
  }

  private void pocessTransaction(final LedgerState<String> ledgerState, final String participantId,
      final String correlationId, final DamlLogEntryId logEntryId, final ByteString envelope)
      throws InternalError, InvalidTransactionException {

    ExecutionContext ec = ExecutionContext.fromExecutor(ExecutionContext.global());
    SubmissionValidator<String> validator = SubmissionValidator.create(ledgerState, () -> {
      return logEntryId;
    }, false, Cache.none(), this.engine, this.metricsRegistry, ec);
    Timestamp recordTime = getRecordTime(ledgerState);
    Future<Either<ValidationFailed, String>> validateAndCommit =
        validator.validateAndCommit(envelope, correlationId, recordTime, participantId);
    CompletionStage<Either<ValidationFailed, String>> validateAndCommitCS =
        FutureConverters.toJava(validateAndCommit);
    try {
      Either<ValidationFailed, String> either = validateAndCommitCS.toCompletableFuture().get();
      if (either.isLeft()) {
        ValidationFailed validationFailed = either.left().get();
        throw new InvalidTransactionException(validationFailed.toString());
      } else {
        String logId = either.right().get();
        LOGGER.info(String.format("Processed transaction into log %s", logId));
      }

    } catch (InterruptedException | ExecutionException e) {
      throw new InternalError(e.getMessage());
    }
  }

  /*
   * private void processTransaction(final LedgerState<?> ledgerState, final TransactionHeader
   * txHeader, final DamlSubmission submission, final String participantId, final DamlLogEntryId
   * entryId) throws InternalError, InvalidTransactionException { final long fetchStateStart =
   * System.currentTimeMillis(); final Map<DamlStateKey, Option<DamlStateValue>> stateMap =
   * buildStateMap(ledgerState, txHeader, submission);
   *
   * final long recordStateStart = System.currentTimeMillis(); recordState(ledgerState, submission,
   * participantId, stateMap, entryId);
   *
   * final long processFinished = System.currentTimeMillis(); final long recordStateTime =
   * processFinished - recordStateStart; final long fetchStateTime = recordStateStart -
   * fetchStateStart;
   * LOGGER.info(String.format("Finished processing transaction %s times=[fetch=%s,record=%s]",
   * txHeader.getPayloadSha512(), fetchStateTime, recordStateTime)); }
   */

  /**
   * Fundamental checks of the transaction.
   *
   * @param tpProcessRequest the process request
   * @throws InvalidTransactionException if the transaction fails because of a business rule
   *                                     validation error
   * @throws InternalError               if the transaction fails because of a system error
   */
  private void basicRequestChecks(final TpProcessRequest tpProcessRequest)
      throws InvalidTransactionException, InternalError {
    LOGGER.info(String.format("Processing transaction %s", tpProcessRequest.getSignature()));

    final TransactionHeader header = tpProcessRequest.getHeader();
    if (header == null) {
      throw new InvalidTransactionException("Header expected");
    }

    final ByteString payload = tpProcessRequest.getPayload();
    if (payload.size() == 0) {
      throw new InvalidTransactionException("Empty payload");
    }

    if (!header.getFamilyName().equals(this.familyName)) {
      throw new InvalidTransactionException("Family name does not match");
    }

    if (!header.getFamilyVersion().contentEquals(this.version)) {
      throw new InvalidTransactionException("Version does not match");
    }
  }

  /*
   * private Map<DamlStateKey, Option<DamlStateValue>> buildStateMap(final LedgerState<?>
   * ledgerState, final TransactionHeader txHeader, final DamlSubmission submission) throws
   * InvalidTransactionException, InternalError {
   * LOGGER.fine(String.format("Fetching DamlState for this transaction")); final Map<DamlStateKey,
   * String> inputDamlStateKeys = KeyValueUtils.submissionToDamlStateAddress(submission);
   *
   * final List<String> inputList = txHeader.getInputsList(); if
   * (!inputList.containsAll(inputDamlStateKeys.values())) { throw new InvalidTransactionException(
   * String.format("Not all input DamlStateKeys were declared as inputs")); } if (!inputList
   * .contains(com.blockchaintp.sawtooth.timekeeper.util.Namespace.TIMEKEEPER_GLOBAL_RECORD)) {
   * throw new InvalidTransactionException(
   * String.format("TIMEKEEPER_GLOBAL_RECORD not declared as input")); } final Map<DamlStateKey,
   * DamlStateValue> inputStates = ledgerState.getDamlStates(inputDamlStateKeys.keySet());
   *
   * final Map<DamlStateKey, Option<DamlStateValue>> inputStatesWithOption = new HashMap<>(); for
   * (DamlStateKey k : inputDamlStateKeys.keySet()) { if (inputStates.containsKey(k)) {
   * LOGGER.fine(String.format("Fetched %s(%s), address=%s", k, k.getKeyCase().toString(),
   * Namespace.makeAddressForType(k))); final Option<DamlStateValue> option =
   * Option.apply(inputStates.get(k)); if (inputStates.get(k).toByteString().size() == 0) {
   * LOGGER.fine(String.format("Fetched %s(%s), address=%s, size=empty", k,
   * k.getKeyCase().toString(), Namespace.makeAddressForType(k))); } else { LOGGER.fine(
   * String.format("Fetched %s(%s), address=%s, size=%s", k, k.getKeyCase().toString(),
   * Namespace.makeAddressForType(k), inputStates.get(k).toByteString().size())); }
   * inputStatesWithOption.put(k, option); } else {
   * LOGGER.fine(String.format("Fetched %s(%s), address=%s, size=empty", k,
   * k.getKeyCase().toString(), Namespace.makeAddressForType(k))); inputStatesWithOption.put(k,
   * Option.empty()); } } return inputStatesWithOption; }
   */
  /*
   * private Configuration getDefaultConfiguration() throws InternalError,
   * InvalidTransactionException { final TimeModel tm =
   * TimeModel.apply(Duration.ofSeconds(DEFAULT_MIN_TX_LATENCY),
   * Duration.ofSeconds(DEFAULT_MAX_CLOCK_SKEW), Duration.ofSeconds(DEFAULT_MAX_TTL),
   * Duration.ofSeconds(DEFAULT_AVG_TX_LATENCY), Duration.ofSeconds(DEFAULT_MIN_SKEW),
   * Duration.ofSeconds(DEFAULT_MAX_SKEW)).get();
   * LOGGER.fine(String.format("Default TimeModel set to %s", tm)); final Configuration
   * blankConfiguration = new Configuration(0, tm); return blankConfiguration; }
   */
  @Override
  public Collection<String> getNameSpaces() {
    return Arrays.asList(new String[] {this.namespace});
  }

  private Timestamp getRecordTime(final LedgerState<?> ledgerState) throws InternalError {
    final com.google.protobuf.Timestamp recordTime = ledgerState.getRecordTime();
    final long micros = Timestamps.toMicros(recordTime);
    return new Timestamp(micros);
  }

  @Override
  public String getVersion() {
    return this.version;
  }

  /*
   * private void recordState(final LedgerState<?> ledgerState, final DamlSubmission submission,
   * final String participantId, final Map<DamlStateKey, Option<DamlStateValue>> stateMap, final
   * DamlLogEntryId entryId) throws InternalError, InvalidTransactionException {
   *
   * final long processStart = System.currentTimeMillis(); String ledgerEffectiveTime = null; String
   * maxRecordTime = null; if (submission.hasTransactionEntry()) { ledgerEffectiveTime = Conversions
   * .parseTimestamp(submission.getTransactionEntry().getLedgerEffectiveTime()).toString();
   * maxRecordTime = Conversions .parseTimestamp(
   * submission.getTransactionEntry().getSubmitterInfo().getMaximumRecordTime()) .toString(); }
   * LOGGER.info(String.format(
   * "Processing submission.  recordTime=%s, ledgerEffectiveTime=%s, maxRecordTime=%s",
   * getRecordTime(ledgerState), ledgerEffectiveTime, maxRecordTime)); final Tuple2<DamlLogEntry,
   * Map<DamlStateKey, DamlStateValue>> processSubmission =
   * this.committer.processSubmission(getDefaultConfiguration(), entryId,
   * getRecordTime(ledgerState), submission, participantId, stateMap); final long recordStart =
   * System.currentTimeMillis(); final Map<DamlStateKey, DamlStateValue> newState =
   * processSubmission._2; ledgerState.setDamlStates(newState.entrySet());
   *
   * final DamlLogEntry newLogEntry = processSubmission._1;
   * LOGGER.fine(String.format("Recording log at %s, address=%s", entryId,
   * Namespace.makeAddressForType(entryId), newLogEntry.toByteString().size()));
   * ledgerState.appendToLog(entryId.toByteArray(), newLogEntry.toByteArray()); final long
   * recordFinish = System.currentTimeMillis(); final long processTime = recordStart - processStart;
   * final long setStateTime = recordFinish - recordStart; final long totalTime = recordFinish -
   * processStart;
   * LOGGER.info(String.format("Record state timings [ total=%s, process=%s, setState=%s ]",
   * totalTime, processTime, setStateTime)); }
   */
  @Override
  public String transactionFamilyName() {
    return this.familyName;
  }

}
