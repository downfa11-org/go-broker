package controller

import (
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/wire"
)

func (ch *CommandHandler) RequestBudget(internal bool) *wire.FrameBudget {
	ch.requestBudgetOnce.Do(func() {
		clientCount, clientBytes := config.DefaultMaxInFlightRequests, config.DefaultMaxRequestBytes
		internalCount, internalBytes := clientCount, clientBytes
		if cfg := ch.Config; cfg != nil {
			if cfg.MaxInFlightRequests > 0 {
				clientCount = cfg.MaxInFlightRequests
			}
			if cfg.MaxRequestBytes > 0 {
				clientBytes = cfg.MaxRequestBytes
			}
			if cfg.MaxInternalInFlightRequests > 0 {
				internalCount = cfg.MaxInternalInFlightRequests
			}
			if cfg.MaxInternalRequestBytes > 0 {
				internalBytes = cfg.MaxInternalRequestBytes
			}
		}
		ch.clientRequestBudget = wire.NewFrameBudget(clientCount, int64(clientBytes))
		ch.internalRequestBudget = wire.NewFrameBudget(internalCount, int64(internalBytes))
	})
	if internal {
		return ch.internalRequestBudget
	}
	return ch.clientRequestBudget
}
