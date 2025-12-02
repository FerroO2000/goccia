package main

import (
	"context"

	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/processor"
)

type canToQuestDBHandler struct {
	processor.CustomHandlerBase
}

func newCANToQuestDBHandler() *canToQuestDBHandler {
	return &canToQuestDBHandler{}
}

func (h *canToQuestDBHandler) Init(_ context.Context) error {
	return nil
}

func (h *canToQuestDBHandler) Handle(_ context.Context, canMsg *processor.CANMessage, qdbMsg *egress.QuestDBMessage) error {
	rows := make([]*egress.QuestDBRow, 0, canMsg.SignalCount)

	for _, sig := range canMsg.Signals {
		valType := sig.Type

		row := egress.NewQuestDBRow(h.getTable(valType))

		row.AddSymbol(egress.NewQuestDBSymbol("name", sig.Name))

		// Allocate columns for can id and raw value, but don't add them yet
		// because in the case of enum signals, we need to add the enum_value column
		// first since it is a symbol column
		columns := make([]egress.QuestDBColumn, 0, 3)
		columns = append(columns, egress.NewQuestDBIntColumn("can_id", int64(sig.CANID)))
		columns = append(columns, egress.NewQuestDBIntColumn("raw_value", int64(sig.RawValue)))

		switch valType {
		case processor.CANSignalValueTypeFlag:
			columns = append(columns, egress.NewQuestDBBoolColumn("flag_value", sig.ValueFlag))

		case processor.CANSignalValueTypeInt:
			columns = append(columns, egress.NewQuestDBIntColumn("integer_value", sig.ValueInt))

		case processor.CANSignalValueTypeFloat:
			columns = append(columns, egress.NewQuestDBFloatColumn("float_value", sig.ValueFloat))

		case processor.CANSignalValueTypeEnum:
			// Add the enum_value column before the can_id and raw_value columns
			row.AddSymbol(egress.NewQuestDBSymbol("enum_value", sig.ValueEnum))
		}

		row.AddColumns(columns...)
		rows = append(rows, row)
	}

	qdbMsg.AddRows(rows...)

	return nil
}

func (h *canToQuestDBHandler) Close() {}

func (h *canToQuestDBHandler) getTable(valType processor.CANSignalValueType) string {
	switch valType {
	case processor.CANSignalValueTypeFlag:
		return "flag_signals"
	case processor.CANSignalValueTypeInt:
		return "int_signals"
	case processor.CANSignalValueTypeFloat:
		return "float_signals"
	case processor.CANSignalValueTypeEnum:
		return "enum_signals"
	default:
		return "unknown_signals"
	}
}
