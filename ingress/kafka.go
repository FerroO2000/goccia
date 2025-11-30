package ingress

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

// KafkaConfig structs contains the configuration for the Kafka ingress stage.
type KafkaConfig struct {
	// The list of broker addresses used to connect to the kafka cluster.
	//
	// Default: "localhost:9092"
	Brokers []string

	// GroupID holds the consumer group id.
	//
	// Default: "group"
	GroupID string

	// Topics allows specifying multiple topics, but can only be used in
	// combination with GroupID, as it is a consumer-group feature. As such, if
	// GroupID is set, then either Topic or Topics must be defined.
	Topics []string

	// An dialer used to open connections to the kafka server. This field is
	// optional, if nil, the default dialer is used instead.
	Dialer *kafka.Dialer

	// The capacity of the internal message queue, defaults to 100 if none is
	// set.
	QueueCapacity int

	// MinBytes indicates to the broker the minimum batch size that the consumer
	// will accept. Setting a high minimum when consuming from a low-volume topic
	// may result in delayed delivery when the broker does not have enough data to
	// satisfy the defined minimum.
	//
	// Default: 1
	MinBytes int

	// MaxBytes indicates to the broker the maximum batch size that the consumer
	// will accept. The broker will truncate a message to satisfy this maximum, so
	// choose a value that is high enough for your largest message size.
	//
	// Default: 1MB
	MaxBytes int

	// Maximum amount of time to wait for new data to come when fetching batches
	// of messages from kafka.
	//
	// Default: 10s
	MaxWait time.Duration

	// ReadBatchTimeout amount of time to wait to fetch message from kafka messages batch.
	//
	// Default: 10s
	ReadBatchTimeout time.Duration

	// GroupBalancers is the priority-ordered list of client-side consumer group
	// balancing strategies that will be offered to the coordinator.  The first
	// strategy that all group members support will be chosen by the leader.
	//
	// Default: [Range, RoundRobin]
	//
	// Only used when GroupID is set
	GroupBalancers []kafka.GroupBalancer

	// HeartbeatInterval sets the optional frequency at which the reader sends the consumer
	// group heartbeat update.
	//
	// Default: 3s
	//
	// Only used when GroupID is set
	HeartbeatInterval time.Duration

	// CommitInterval indicates the interval at which offsets are committed to
	// the broker.  If 0, commits will be handled synchronously.
	//
	// Default: 0
	//
	// Only used when GroupID is set
	CommitInterval time.Duration

	// PartitionWatchInterval indicates how often a reader checks for partition changes.
	// If a reader sees a partition change (such as a partition add) it will rebalance the group
	// picking up new partitions.
	//
	// Default: 5s
	//
	// Only used when GroupID is set and WatchPartitionChanges is set.
	PartitionWatchInterval time.Duration

	// WatchForPartitionChanges is used to inform kafka-go that a consumer group should be
	// polling the brokers and rebalancing if any partition changes happen to the topic.
	WatchPartitionChanges bool

	// SessionTimeout optionally sets the length of time that may pass without a heartbeat
	// before the coordinator considers the consumer dead and initiates a rebalance.
	//
	// Default: 30s
	//
	// Only used when GroupID is set
	SessionTimeout time.Duration

	// RebalanceTimeout optionally sets the length of time the coordinator will wait
	// for members to join as part of a rebalance.  For kafka servers under higher
	// load, it may be useful to set this value higher.
	//
	// Default: 30s
	//
	// Only used when GroupID is set
	RebalanceTimeout time.Duration

	// JoinGroupBackoff optionally sets the length of time to wait between re-joining
	// the consumer group after an error.
	//
	// Default: 5s
	JoinGroupBackoff time.Duration

	// RetentionTime optionally sets the length of time the consumer group will be saved
	// by the broker. -1 will disable the setting and leave the
	// retention up to the broker's offsets.retention.minutes property. By
	// default, that setting is 1 day for kafka < 2.0 and 7 days for kafka >= 2.0.
	//
	// Default: -1
	//
	// Only used when GroupID is set
	RetentionTime time.Duration

	// StartOffset determines from whence the consumer group should begin
	// consuming when it finds a partition without a committed offset.  If
	// non-zero, it must be set to one of FirstOffset or LastOffset.
	//
	// Default: FirstOffset
	//
	// Only used when GroupID is set
	StartOffset int64

	// BackoffDelayMin optionally sets the smallest amount of time the reader will wait before
	// polling for new messages
	//
	// Default: 100ms
	ReadBackoffMin time.Duration

	// BackoffDelayMax optionally sets the maximum amount of time the reader will wait before
	// polling for new messages
	//
	// Default: 1s
	ReadBackoffMax time.Duration

	// IsolationLevel controls the visibility of transactional records.
	// ReadUncommitted makes all records visible. With ReadCommitted only
	// non-transactional and committed records are visible.
	IsolationLevel kafka.IsolationLevel

	// Limit of how many attempts to connect will be made before returning the error.
	//
	// The default is to try 3 times.
	MaxAttempts int
}

// DefaultKafkaConfig returns a default kafka config.
// There are NO default topics set.
func DefaultKafkaConfig(topics ...string) *KafkaConfig {
	groupBalancer := []kafka.GroupBalancer{
		kafka.RangeGroupBalancer{},
		kafka.RoundRobinGroupBalancer{},
	}

	return &KafkaConfig{
		Brokers:                []string{"localhost:9092"},
		GroupID:                "group",
		Topics:                 topics,
		QueueCapacity:          100,
		MinBytes:               1,
		MaxBytes:               10e6,
		MaxWait:                10 * time.Second,
		ReadBatchTimeout:       10 * time.Second,
		GroupBalancers:         groupBalancer,
		HeartbeatInterval:      3 * time.Second,
		CommitInterval:         0,
		PartitionWatchInterval: 5 * time.Second,
		WatchPartitionChanges:  false,
		SessionTimeout:         30 * time.Second,
		RebalanceTimeout:       30 * time.Second,
		JoinGroupBackoff:       5 * time.Second,
		RetentionTime:          time.Hour * 24 * 7,
		StartOffset:            kafka.FirstOffset,
		ReadBackoffMin:         100 * time.Millisecond,
		ReadBackoffMax:         1 * time.Second,
		IsolationLevel:         kafka.ReadUncommitted,
		MaxAttempts:            3,
	}
}

///////////////
//  MESSAGE  //
///////////////

var _ msgEnv = (*KafkaMessage)(nil)

// KafkaMessage represents a message returned by the Kafka ingress stage.
type KafkaMessage struct {
	Topic string
	Key   []byte
	Value []byte

	Headers []kafka.Header
}

func newKafkaMessage() *KafkaMessage {
	return &KafkaMessage{}
}

// Destroy cleans up the message.
func (km *KafkaMessage) Destroy() {}

//////////////
//  SOURCE  //
//////////////

var _ source[*KafkaMessage] = (*kafkaSource)(nil)

type kafkaSource struct {
	tel *internal.Telemetry

	reader *kafka.Reader

	// Metrics
	receivedMessages atomic.Int64
	receivedBytes    atomic.Int64
}

func newKafkaSource() *kafkaSource {
	return &kafkaSource{}
}

func (ks *kafkaSource) setTelemetry(tel *internal.Telemetry) {
	ks.tel = tel
}

func (ks *kafkaSource) init(readerCfg kafka.ReaderConfig) {
	ks.reader = kafka.NewReader(readerCfg)

	ks.initMetrics()
}

func (ks *kafkaSource) initMetrics() {
	ks.tel.NewCounter("received_messages", func() int64 { return ks.receivedMessages.Load() })
	ks.tel.NewCounter("received_bytes", func() int64 { return ks.receivedBytes.Load() })
}

func (ks *kafkaSource) run(ctx context.Context, outConnector msgConn[*KafkaMessage]) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := ks.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			ks.tel.LogError("failed to read message", err)
			continue
		}

		msgOut := ks.handleMessage(ctx, &msg)
		if err := outConnector.Write(msgOut); err != nil {
			msgOut.Destroy()
			ks.tel.LogError("failed to write message to output connector", err)
		}

		ks.receivedMessages.Add(1)
	}
}

func (ks *kafkaSource) handleMessage(ctx context.Context, msg *kafka.Message) *msg[*KafkaMessage] {
	if len(msg.Headers) > 0 {
		headerCarrier := telemetry.NewKafkaHeaderCarrier(msg.Headers)
		ctx = ks.tel.ExtractTraceContext(ctx, headerCarrier)
	}

	_, span := ks.tel.NewTrace(ctx, "handle kafka message")
	defer span.End()

	kafkaMsg := newKafkaMessage()

	kafkaMsg.Topic = msg.Topic
	kafkaMsg.Key = msg.Key
	kafkaMsg.Value = msg.Value
	kafkaMsg.Headers = msg.Headers

	msgRes := message.NewMessage(kafkaMsg)

	recvTime := time.Now()
	msgRes.SetReceiveTime(recvTime)
	msgRes.SetTimestamp(recvTime)

	valueSize := len(msg.Value)

	span.SetAttributes(attribute.Int("value_size", valueSize))
	msgRes.SaveSpan(span)

	ks.receivedBytes.Add(int64(valueSize))

	return msgRes
}

func (ks *kafkaSource) close() {
	if err := ks.reader.Close(); err != nil {
		ks.tel.LogError("failed to close reader", err)
	}
}

/////////////
//  STAGE  //
/////////////

// KafkaStage is an ingress stage that reads messages from Kafka.
type KafkaStage struct {
	*stage[*KafkaMessage]

	cfg *KafkaConfig

	source *kafkaSource
}

// NewKafkaStage returns a new Kafka ingress stage.
func NewKafkaStage(outConnector msgConn[*KafkaMessage], cfg *KafkaConfig) *KafkaStage {
	source := newKafkaSource()

	return &KafkaStage{
		stage: newStage("kafka", source, outConnector),

		cfg: cfg,

		source: source,
	}
}

// Init initializes the stage.
func (ks *KafkaStage) Init(ctx context.Context) error {
	ks.source.init(kafka.ReaderConfig{
		Brokers:                ks.cfg.Brokers,
		GroupID:                ks.cfg.GroupID,
		GroupTopics:            ks.cfg.Topics,
		Dialer:                 ks.cfg.Dialer,
		QueueCapacity:          ks.cfg.QueueCapacity,
		MinBytes:               ks.cfg.MinBytes,
		MaxBytes:               ks.cfg.MaxBytes,
		MaxWait:                ks.cfg.MaxWait,
		ReadBatchTimeout:       ks.cfg.ReadBatchTimeout,
		GroupBalancers:         ks.cfg.GroupBalancers,
		HeartbeatInterval:      ks.cfg.HeartbeatInterval,
		CommitInterval:         ks.cfg.CommitInterval,
		PartitionWatchInterval: ks.cfg.PartitionWatchInterval,
		WatchPartitionChanges:  ks.cfg.WatchPartitionChanges,
		SessionTimeout:         ks.cfg.SessionTimeout,
		RebalanceTimeout:       ks.cfg.RebalanceTimeout,
		JoinGroupBackoff:       ks.cfg.JoinGroupBackoff,
		RetentionTime:          ks.cfg.RetentionTime,
		StartOffset:            ks.cfg.StartOffset,
		ReadBackoffMin:         ks.cfg.ReadBackoffMin,
		ReadBackoffMax:         ks.cfg.ReadBackoffMax,
		IsolationLevel:         ks.cfg.IsolationLevel,
		MaxAttempts:            ks.cfg.MaxAttempts,
	})

	return ks.stage.Init(ctx)
}

// Close closes the stage.
func (ks *KafkaStage) Close() {
	ks.stage.Close()
	ks.source.close()
}
