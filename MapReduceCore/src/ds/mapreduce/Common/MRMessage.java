package ds.mapreduce.Common;

import java.io.Serializable;

public class MRMessage implements Serializable {
	private static final long serialVersionUID = 1L;
	Command cmd;
	Object payload;

	public MRMessage(Command c, Object p) {
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