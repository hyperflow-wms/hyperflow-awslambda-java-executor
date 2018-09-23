package payloads;

import java.util.List;
import java.util.Map;

public class Request {

    private String executable;
    private List<String> args;
    private Map<String, String> env;
    private List<Map<String, Object>> inputs;
    private List<Map<String, String>> outputs;
    private Map<String, String> options;

    public void setExecutable(String executable) {
        this.executable = executable;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public void setEnv(Map<String, String> env) {
        this.env = env;
    }

    public void setInputs(List<Map<String, Object>> inputs) {
        this.inputs = inputs;
    }

    public void setOutputs(List<Map<String, String>> outputs) {
        this.outputs = outputs;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    public String getExecutable() {
        return executable;
    }

    public List<String> getArgs() {
        return args;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public List<Map<String, Object>> getInputs() {
        return inputs;
    }

    public List<Map<String, String>> getOutputs() {
        return outputs;
    }

    public Map<String, String> getOptions() {
        return options;

    }
}
