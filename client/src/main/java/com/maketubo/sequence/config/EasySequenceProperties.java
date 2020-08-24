package com.maketubo.sequence.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "easy-sequence", ignoreUnknownFields = false)
public class EasySequenceProperties {
    /**
     * 是否启用
     */
    private boolean enabled = true;

    /**
     * 是否分类
     */
    private boolean category = false;

    private SequenceMode mode = SequenceMode.redis;

    private boolean external = false;

    private String type = null;

    private String zookeeperServer = "127.0.0.1";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isCategory() {
        return category;
    }

    public void setCategory(boolean category) {
        this.category = category;
    }

    public SequenceMode getMode() {
        return mode;
    }

    public void setMode(SequenceMode mode) {
        this.mode = mode;
    }

    public boolean isExternal() {
        return external;
    }

    public void setExternal(boolean external) {
        this.external = external;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Class<?> getProvider(){
        try {
            return Class.forName(this.type);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("please check the type is exists", e);
        }
    }

    public String getZookeeperServer() {
        return zookeeperServer;
    }

    public void setZookeeperServer(String zookeeperServer) {
        this.zookeeperServer = zookeeperServer;
    }
}
