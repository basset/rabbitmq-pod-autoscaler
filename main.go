package main

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/streadway/amqp"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Config struct {
	RabbitMQHost string
	QueueName    string
	Namespace    string
	Deployment   string
	MaxPods      int
	MinPods      int
	MsgPerPod    int
	ScanInterval int
	ScaleFactor  float64
}

var configKeys = struct {
	RabbitMQHost string
	QueueName    string
	Namespace    string
	Deployment   string
	MaxPods      string
	MinPods      string
	MsgPerPod    string
	ScanInterval string
	ScaleFactor  string
}{
	RabbitMQHost: "AMQP_HOST",
	QueueName:    "AMQP_BUILD_QUEUE",
	Namespace:    "NAMESPACE",
	Deployment:   "DEPLOYMENT",
	MaxPods:      "MAX_PODS",
	MinPods:      "MIN_PODS",
	MsgPerPod:    "MSG_PER_POD",
	ScanInterval: "SCAN_INTERVAL",
	ScaleFactor:  "SCALE_FACTOR",
}

func main() {
	config := Config{}
	debug := os.Getenv("DEBUG") == "true"
	rConfigKeys := reflect.ValueOf(configKeys)
	typeOfConfigKeys := rConfigKeys.Type()
	pConfig := reflect.ValueOf(&config)
	configStruct := pConfig.Elem()

	for i := 0; i < rConfigKeys.NumField(); i++ {
		envName := rConfigKeys.Field(i).Interface()
		fieldName := typeOfConfigKeys.Field(i).Name
		configField := configStruct.FieldByName(fieldName)
		if !configField.IsValid() {
			panic(fmt.Errorf("[Config] Field %s is not valid", fieldName))
		}
		if !configField.CanSet() {
			panic(fmt.Errorf("[Config] Field %s is cannot be set", fieldName))
		}
		value := os.Getenv(envName.(string))
		if value == "" {
			panic(fmt.Errorf("[Config] Envrionmental variable %s is not set", envName))
		}
		if configField.Kind() == reflect.Int {
			number, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				panic(fmt.Errorf("[Config] Field %s is not a valid int", fieldName))
			}
			configField.SetInt(number)
		}
		if configField.Kind() == reflect.String {
			configField.SetString(value)
		}
		if configField.Kind() == reflect.Float64 {
			number, err := strconv.ParseFloat(value, 64)
			if err != nil {
				panic(fmt.Errorf("[Config] Field %s is not a valid float64", fieldName))
			}
			configField.SetFloat(number)
		}
	}

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		panic(fmt.Errorf("[Kube]: Error creating config: %s", err))
	}

	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		panic(fmt.Errorf("[Kube]: Error creating client: %s", err))
	}

	conn, err := amqp.Dial(config.RabbitMQHost)
	if err != nil {
		panic(fmt.Errorf("[RabbitMQ] Failed to connect: %s", err))
	}
	ch, err := conn.Channel()
	if err != nil {
		panic(fmt.Errorf("[RabbitMQ] Failed to open a channel: %s", err))
	}

	for {
		queue, err := ch.QueueInspect(config.QueueName)
		if err != nil {
			ch.Close()
			conn.Close()
			panic(fmt.Errorf("[RabbitMQ]: Error inspecting queue: %s", err))
		}
		messageCount := queue.Messages

		if debug {
			println(fmt.Sprintf("[Debug] [RabbitMQ] Message Count: %d", messageCount))
		}

		client := clientset.AppsV1().Deployments(config.Namespace)
		currentScale, err := client.GetScale(config.Deployment, metav1.GetOptions{})
		if err != nil {
			panic(fmt.Errorf("[Kube]: Error getting scale: %s", err))
		}

		currentReplicas := int(currentScale.Spec.Replicas)
		replicas := int(math.Ceil(float64(messageCount) / float64(config.MsgPerPod)))

		if replicas < config.MinPods {
			replicas = config.MinPods
		}
		if replicas > config.MaxPods {
			replicas = config.MaxPods
		}

		if debug {
			println(fmt.Sprintf("[Debug] [Kube] Current replicas: %d", currentReplicas))
			println(fmt.Sprintf("[Debug] [Kube] Desired replicas: %d", replicas))
		}
		if replicas < currentReplicas {
			desiredReplicas := int(math.Floor(float64(currentReplicas-replicas) * config.ScaleFactor))
			replicas = desiredReplicas + replicas
			if debug {
				println(fmt.Sprintf("[Debug] [Kube] Scale down %f replicas: %d", config.ScaleFactor, replicas))
			}
		}
		if replicas != currentReplicas {
			scale := autoscalingv1.Scale{
				ObjectMeta: metav1.ObjectMeta{
					Name:      currentScale.Name,
					Namespace: currentScale.Namespace,
				},
				Spec: autoscalingv1.ScaleSpec{
					Replicas: int32(replicas),
				},
			}
			_, err = client.UpdateScale(config.Deployment, &scale)
			if err != nil {
				panic(fmt.Errorf("[Kube] Error scaling: %s", err))
			}
			println(fmt.Sprintf("[Kube]: Scaled to %d replicas", replicas))
		}
		time.Sleep(time.Duration(config.ScanInterval) * time.Second)
	}
}
