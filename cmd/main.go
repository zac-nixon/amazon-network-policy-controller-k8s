/*
Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"net/http"
	"os"

	"github.com/go-logr/logr"
	"github.com/spf13/pflag"
	"go.uber.org/zap/zapcore"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "net/http/pprof"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	policyinfo "github.com/aws/amazon-network-policy-controller-k8s/api/v1alpha1"
	"github.com/aws/amazon-network-policy-controller-k8s/internal/app"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/config"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/version"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(policyinfo.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	infoLogger := getLoggerWithLogLevel("info")
	infoLogger.Info("version",
		"GitVersion", version.GitVersion,
		"GitCommit", version.GitCommit,
		"BuildDate", version.BuildDate,
	)
	controllerCFG, err := loadControllerConfig()
	if err != nil {
		infoLogger.Error(err, "unable to load controller config")
		os.Exit(1)
	}
	ctrlLogger := getLoggerWithLogLevel(controllerCFG.LogLevel)
	ctrl.SetLogger(ctrlLogger)

	restCFG, err := config.BuildRestConfig(controllerCFG.RuntimeConfig)
	if err != nil {
		setupLog.Error(err, "unable to build REST config")
		os.Exit(1)
	}

	clientSetRestConfig, err := config.BuildRestConfig(controllerCFG.RuntimeConfig)
	if err != nil {
		setupLog.Error(err, "unable to build REST config")
		os.Exit(1)
	}
	clientSetRestConfig.AcceptContentTypes = "application/vnd.kubernetes.protobuf,application/json"
	clientSetRestConfig.ContentType = "application/vnd.kubernetes.protobuf"

	clientSet, err := kubernetes.NewForConfig(clientSetRestConfig)
	if err != nil {
		setupLog.Error(err, "unable to obtain clientSet")
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()

	if controllerCFG.EnableGoProfiling {
		go func() {
			if err := http.ListenAndServe("localhost:6060", nil); err != nil {
				setupLog.Error(err, "Error starting HTTP server")
			}
		}()
	}

	if err := app.Run(ctx, controllerCFG, app.NewDependencies(controllerCFG, restCFG, clientSet, scheme)); err != nil {
		setupLog.Error(err, "controller exited with error")
		os.Exit(1)
	}
}

// loadControllerConfig loads the controller configuration
func loadControllerConfig() (config.ControllerConfig, error) {
	controllerConfig := config.ControllerConfig{}
	fs := pflag.NewFlagSet("", pflag.ExitOnError)
	controllerConfig.BindFlags(fs)

	if err := fs.Parse(os.Args); err != nil {
		return controllerConfig, err
	}

	return controllerConfig, nil
}

// getLoggerWithLogLevel returns logger with specific log level.
func getLoggerWithLogLevel(logLevel string) logr.Logger {
	var zapLevel zapcore.Level
	switch logLevel {
	case "info":
		zapLevel = zapcore.InfoLevel
	case "debug":
		zapLevel = zapcore.DebugLevel
	default:
		zapLevel = zapcore.InfoLevel
	}
	return zap.New(zap.UseDevMode(false),
		zap.Level(zapLevel),
		zap.StacktraceLevel(zapcore.FatalLevel),
	)
}
