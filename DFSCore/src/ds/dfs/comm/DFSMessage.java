package ds.dfs.comm;

import java.io.Serializable;

import ds.dfs.util.Command;

public class DFSMessage implements Serializable {

	private static final long serialVersionUID = 1L;
	Command cmd;
	Object payload;

	public DFSMessage(Command c, Object p) {
		cmd = c;
		payload = p;
	}

	public Object getPayload() {
		return payload;
	}

	public Command getCommand() {
		return cmd;
	}

}
