package clientwrappers

import (
	"github.com/konpyutaika/nifikop/pkg/nificlient"
	"go.uber.org/zap"
)

// All logging here is debug since all clients report errors in log messages already.
// These can be seen through debug logging.

func ErrorUpdateOperation(log *zap.Logger, err error, action string) error {
	if err == nificlient.ErrNifiClusterNotReturned200 {
		log.Debug("failed since Nifi node returned non 200",
			zap.String("action", action),
			zap.Error(err))
	}

	if err != nil {
		log.Debug("could not communicate with nifi node",
			zap.String("action", action),
			zap.Error(err))
	}
	return err
}

func ErrorGetOperation(log *zap.Logger, err error, action string) error {
	if err == nificlient.ErrNifiClusterNotReturned200 {
		log.Debug("failed since Nifi node returned non 200",
			zap.String("action", action),
			zap.Error(err))
	}

	if err != nil {
		log.Debug("could not communicate with nifi node",
			zap.String("action", action),
			zap.Error(err))
	}

	return err
}

func ErrorCreateOperation(log *zap.Logger, err error, action string) error {
	if err == nificlient.ErrNifiClusterNotReturned201 {
		log.Debug("failed since Nifi node returned non 201",
			zap.String("action", action),
			zap.Error(err))
	}

	if err != nil {
		log.Debug("could not communicate with nifi node",
			zap.String("action", action),
			zap.Error(err))
	}

	return err
}

func ErrorRemoveOperation(log *zap.Logger, err error, action string) error {
	if err == nificlient.ErrNifiClusterNotReturned200 {
		log.Debug("failed since Nifi node returned non 200",
			zap.String("action", action),
			zap.Error(err))
	}

	if err != nil {
		log.Debug("could not communicate with nifi node",
			zap.String("action", action),
			zap.Error(err))
	}

	return err
}
