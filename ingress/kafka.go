package ingress

import (
	"context"
	"errors"
	"time"

	"github.com/FerroO2000/goccia/ingress/metrics"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// DefaultKafkaConfigBrokers is the default list of Kafka brokers to connect to.
var DefaultKafkaConfigBrokers = []string{"localhost:9092"}

// Default values for the Kafka ingress stage configuration.
const (
	DefaultKafkaConfigGroupID                = "group"
	DefaultKafkaConfigQueueCapacity          = 100
	DefaultKafkaConfigMinBytes               = 1
	DefaultKafkaConfigMaxBytes               = 1 << 20
	DefaultKafkaConfigMaxWait                = 10 * time.Second
	DefaultKafkaConfigReadBatchTimeout       = 10 * time.Second
	DefaultKafkaConfigHeartbeatInterval      = 3 * time.Second
	DefaultKafkaConfigCommitInterval         = 0
	DefaultKafkaConfigPartitionWatchInterval = 5 * time.Second
	DefaultKafkaConfigWatchPartitionChanges  = false
	DefaultKafkaConfigSessionTimeout         = 30 * time.Second
	DefaultKafkaConfigRebalanceTimeout       = 30 * time.Second
	DefaultKafkaConfigJoinGroupBackoff       = 5 * time.Second
	DefaultKafkaConfigRetentionTime          = time.Hour * 24 * 7
	DefaultKafkaConfigStartOffset            = kafka.FirstOffset
	DefaultKafkaConfigReadMinBackoff         = 100 * time.Millisecond
	DefaultKafkaConfigReadMaxBackoff         = 1 * time.Second
	DefaultKafkaConfigIsolationLevel         = kafka.ReadUncommitted
	DefaultKafkaConfigMaxAttempts            = 3
)

// DefaultKafkaConfigGroupBalancer is the default balancer used to distribute messages across partitions.
var DefaultKafkaConfigGroupBalancer = []kafka.GroupBalancer{
	kafka.RangeGroupBalancer{},
	kafka.RoundRobinGroupBalancer{},
}

// KafkaConfig structs contains the configuration for the Kafka ingress stage.
type KafkaConfig struct {
	// The list of broker addresses used to connect to the kafka cluster.
	Brokers []string

	// GroupID holds the consumer group id.
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
	MinBytes int

	// MaxBytes indicates to the broker the maximum batch size that the consumer
	// will accept. The broker will truncate a message to satisfy this maximum, so
	// choose a value that is high enough for your largest message size.
	MaxBytes int

	// Maximum amount of time to wait for new data to come when fetching batches
	// of messages from kafka.
	MaxWait time.Duration

	// ReadBatchTimeout amount of time to wait to fetch message from kafka messages batch.
	ReadBatchTimeout time.Duration

	// GroupBalancers is the priority-ordered list of client-side consumer group
	// balancing strategies that will be offered to the coordinator.  The first
	// strategy that all group members support will be chosen by the leader.
	//
	// Only used when GroupID is set
	GroupBalancers []kafka.GroupBalancer

	// HeartbeatInterval sets the optional frequency at which the reader sends the consumer
	// group heartbeat update.
	//
	// Only used when GroupID is set
	HeartbeatInterval time.Duration

	// CommitInterval indicates the interval at which offsets are committed to
	// the broker.  If 0, commits will be handled synchronously.
	//
	// Only used when GroupID is set
	CommitInterval time.Duration

	// PartitionWatchInterval indicates how often a reader checks for partition changes.
	// If a reader sees a partition change (such as a partition add) it will rebalance the group
	// picking up new partitions.
	//
	// Only used when GroupID is set and WatchPartitionChanges is set.
	PartitionWatchInterval time.Duration

	// WatchForPartitionChanges is used to inform kafka-go that a consumer group should be
	// polling the brokers and rebalancing if any partition changes happen to the topic.
	WatchPartitionChanges bool

	// SessionTimeout optionally sets the length of time that may pass without a heartbeat
	// before the coordinator considers the consumer dead and initiates a rebalance.
	//
	// Only used when GroupID is set
	SessionTimeout time.Duration

	// RebalanceTimeout optionally sets the length of time the coordinator will wait
	// for members to join as part of a rebalance.  For kafka servers under higher
	// load, it may be useful to set this value higher.
	//
	// Only used when GroupID is set
	RebalanceTimeout time.Duration

	// JoinGroupBackoff optionally sets the length of time to wait between re-joining
	// the consumer group after an error.
	JoinGroupBackoff time.Duration

	// RetentionTime optionally sets the length of time the consumer group will be saved
	// by the broker. -1 will disable the setting and leave the
	// retention up to the broker's offsets.retention.minutes property. By
	// default, that setting is 1 day for kafka < 2.0 and 7 days for kafka >= 2.0.
	//
	// Only used when GroupID is set
	RetentionTime time.Duration

	// StartOffset determines from whence the consumer group should begin
	// consuming when it finds a partition without a committed offset.  If
	// non-zero, it must be set to one of FirstOffset or LastOffset.
	//
	// Only used when GroupID is set
	StartOffset int64

	// BackoffDelayMin optionally sets the smallest amount of time the reader will wait before
	// polling for new messages
	ReadBackoffMin time.Duration

	// BackoffDelayMax optionally sets the maximum amount of time the reader will wait before
	// polling for new messages
	ReadBackoffMax time.Duration

	// IsolationLevel controls the visibility of transactional records.
	// ReadUncommitted makes all records visible. With ReadCommitted only
	// non-transactional and committed records are visible.
	IsolationLevel kafka.IsolationLevel

	// Limit of how many attempts to connect will be made before returning the error.
	MaxAttempts int
}

// DefaultKafkaConfig returns a default kafka config.
// There are NO default topics set.
func DefaultKafkaConfig(topics ...string) *KafkaConfig {
	return &KafkaConfig{
		Brokers:                DefaultKafkaConfigBrokers,
		GroupID:                DefaultKafkaConfigGroupID,
		Topics:                 topics,
		QueueCapacity:          DefaultKafkaConfigQueueCapacity,
		MinBytes:               DefaultKafkaConfigMinBytes,
		MaxBytes:               DefaultKafkaConfigMaxBytes,
		MaxWait:                DefaultKafkaConfigMaxWait,
		ReadBatchTimeout:       DefaultKafkaConfigReadBatchTimeout,
		GroupBalancers:         DefaultKafkaConfigGroupBalancer,
		HeartbeatInterval:      DefaultKafkaConfigHeartbeatInterval,
		CommitInterval:         DefaultKafkaConfigCommitInterval,
		PartitionWatchInterval: DefaultKafkaConfigPartitionWatchInterval,
		WatchPartitionChanges:  DefaultKafkaConfigWatchPartitionChanges,
		SessionTimeout:         DefaultKafkaConfigSessionTimeout,
		RebalanceTimeout:       DefaultKafkaConfigRebalanceTimeout,
		JoinGroupBackoff:       DefaultKafkaConfigJoinGroupBackoff,
		RetentionTime:          DefaultKafkaConfigRetentionTime,
		StartOffset:            DefaultKafkaConfigStartOffset,
		ReadBackoffMin:         DefaultKafkaConfigReadMinBackoff,
		ReadBackoffMax:         DefaultKafkaConfigReadMaxBackoff,
		IsolationLevel:         DefaultKafkaConfigIsolationLevel,
		MaxAttempts:            DefaultKafkaConfigMaxAttempts,
	}
}

// Validate checks the configuration.
func (c *KafkaConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckLen(ac, "Brokers", &c.Brokers, DefaultKafkaConfigBrokers)

	config.CheckNotEmpty(ac, "GroupID", &c.GroupID, DefaultKafkaConfigGroupID)
}

// ─── Message ────────────────────────────────────────────────────────────────|

var _ msgSer = (*KafkaMessage)(nil)

// KafkaMessage represents a message returned by the Kafka ingress stage.
type KafkaMessage struct {
	Topic string
	Key   []byte
	Value []byte

	Headers []kafka.Header
}

// NewKafkaMessage returns a new Kafka message.
func NewKafkaMessage() *KafkaMessage {
	return &KafkaMessage{}
}

// Destroy cleans up the message.
func (km *KafkaMessage) Destroy() {}

// GetBytes returns the bytes of the Kafka's message value.
func (km *KafkaMessage) GetBytes() []byte {
	return km.Value
}

// ─── Environment ────────────────────────────────────────────────────────────|

type kafkaEnv struct {
	*env.BaseEnv[*KafkaConfig, *metrics.KafkaStage]
}

func newKafkaEnv(config *KafkaConfig) *kafkaEnv {
	return &kafkaEnv{
		BaseEnv: env.NewIngressEnv(config, metrics.NewKafkaStage()),
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*kafkaEnv] = (*kafkaRunner)(nil)

type kafkaRunner struct {
	*runnerBase[*kafkaEnv, *KafkaMessage]

	reader *kafka.Reader
}

func newKafkaRunner(outConnector msgConn[*KafkaMessage]) *kafkaRunner {
	return &kafkaRunner{
		runnerBase: newRunnerBase[*kafkaEnv](outConnector),
	}
}

func (kr *kafkaRunner) Init(_ context.Context) error {
	kr.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:                kr.env.Config.Brokers,
		GroupID:                kr.env.Config.GroupID,
		GroupTopics:            kr.env.Config.Topics,
		Dialer:                 kr.env.Config.Dialer,
		QueueCapacity:          kr.env.Config.QueueCapacity,
		MinBytes:               kr.env.Config.MinBytes,
		MaxBytes:               kr.env.Config.MaxBytes,
		MaxWait:                kr.env.Config.MaxWait,
		ReadBatchTimeout:       kr.env.Config.ReadBatchTimeout,
		GroupBalancers:         kr.env.Config.GroupBalancers,
		HeartbeatInterval:      kr.env.Config.HeartbeatInterval,
		CommitInterval:         kr.env.Config.CommitInterval,
		PartitionWatchInterval: kr.env.Config.PartitionWatchInterval,
		WatchPartitionChanges:  kr.env.Config.WatchPartitionChanges,
		SessionTimeout:         kr.env.Config.SessionTimeout,
		RebalanceTimeout:       kr.env.Config.RebalanceTimeout,
		JoinGroupBackoff:       kr.env.Config.JoinGroupBackoff,
		RetentionTime:          kr.env.Config.RetentionTime,
		StartOffset:            kr.env.Config.StartOffset,
		ReadBackoffMin:         kr.env.Config.ReadBackoffMin,
		ReadBackoffMax:         kr.env.Config.ReadBackoffMax,
		IsolationLevel:         kr.env.Config.IsolationLevel,
		MaxAttempts:            kr.env.Config.MaxAttempts,
	})

	return nil
}

func (kr *kafkaRunner) Run(ctx context.Context) {
	defer kr.notifyRunDone()

	for {
		msg, err := kr.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			kr.env.Tel.LogError("failed to read message", err)
			continue
		}

		msgOut := kr.handleMessage(ctx, &msg)
		if err := kr.outConnector.Write(msgOut); err != nil {
			msgOut.Destroy()
			kr.env.Tel.LogError("failed to write message to output connector", err)
		}

		kr.env.Metrics.IncrementReceivedMessages()
	}
}

func (kr *kafkaRunner) handleMessage(ctx context.Context, msg *kafka.Message) *msg[*KafkaMessage] {
	if len(msg.Headers) > 0 {
		headerCarrier := telemetry.NewKafkaHeaderCarrier(msg.Headers)
		ctx = kr.env.Tel.ExtractTraceContext(ctx, headerCarrier)
	}

	_, span := kr.env.Tel.StartTrace(ctx, "handle kafka message")
	defer span.End()

	kafkaMsg := NewKafkaMessage()

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

	kr.env.Metrics.AddReceivedBytes(uint(valueSize))

	return msgRes
}

func (kr *kafkaRunner) Close(ctx context.Context) {
	kr.Close(ctx)
	kr.reader.Close()
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// KafkaStage is an ingress stage that reads messages from Kafka.
type KafkaStage struct {
	*stage.IngressStage[*KafkaMessage, *kafkaEnv]
}

// NewKafkaStage returns a new Kafka ingress stage.
func NewKafkaStage(outConnector msgConn[*KafkaMessage], cfg *KafkaConfig) *KafkaStage {
	return &KafkaStage{
		IngressStage: stage.NewIngressStageFromRunner[*KafkaMessage](
			"kafka", newKafkaEnv(cfg), newKafkaRunner(outConnector),
		),
	}
}
