package core

// CurrentTools returns copies of the tools a step starting now would
// resolve calls against: a fresh fetch of the ToolSource when the agent
// has one — validated as a step's snapshot is, so a nil entry fails
// with ErrNilTool and a duplicate name with ErrDuplicateTool — and the
// registered set (Tools) otherwise. It answers what the agent holds
// now, not what one step showed: a PrepareStep rewrite and a run's
// OnlyTools narrowing are not applied.
//
// It is for code that checks a parked or cut-off call against the tool
// it will run under (weft/thread: the approval drift check, the
// recovery ladder's replay class) and must see a ToolSource's tools,
// which Tools and Manifest leave out. Each call fetches the source
// once; the source is the caller's code and runs on the caller's
// goroutine.
func (a *Agent) CurrentTools() ([]*ToolDef, error) {
	tools, err := a.dispatchTools()
	if err != nil {
		return nil, err
	}
	out := make([]*ToolDef, len(tools))
	for i, t := range tools {
		out[i] = t.clone()
	}
	return out, nil
}
