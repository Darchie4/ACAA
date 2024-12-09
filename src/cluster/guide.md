
## Deploy Kafka Connect, Schema Registry, and KSQL.

1. **Deploy services:**
   - Apply manifests:
     ```bash
     kubectl apply -f kafka-schema-registry.yaml
     kubectl apply -f kafka-connect.yaml
     kubectl apply -f kafka-ksqldb.yaml
     ```

2. **Enable Kafka modules in `redpanda.yaml`:**
   ```yaml
   KAFKA_SCHEMAREGISTRY_ENABLED=true
   CONNECT_ENABLED=true
   ```

#### **Interacting with Services**

1. **Kafka Schema Registry:**
   - Forward port:
     ```bash
     kubectl port-forward svc/kafka-schema-registry 8081
     ```
   - Test using `curl`:
     ```bash
     curl http://127.0.0.1:8081
     ```

2. **Kafka Connect:**
   - Forward port:
     ```bash
     kubectl port-forward svc/kafka-connect 8083
     ```
   - Test using `curl`:
     ```bash
     curl http://127.0.0.1:8083
     ```

3. **KSQL:**
   - Access the CLI:
     ```bash
     kubectl exec --stdin --tty deployment/kafka-ksqldb-cli -- ksql http://kafka-ksqldb-server:8088
     ```

---

### **Redpanda Console**

#### Use Redpanda Console for Kafka interaction.

1. **Deploy Redpanda:**
   ```bash
   kubectl apply -f redpanda.yaml
   ```

2. **Access Redpanda:**
   ```bash
   kubectl port-forward svc/redpanda 8080
   ```

3. **Manual interaction:**
   - **Create a topic:**
     - Use the **Create Topic** button in the Topics view.
   - **Produce records:**
     - In the topic view, select **Produce Record** and input:
       - Key: `1`
       - Value: `{"id":1,"status":"it works"}`
   - **Delete records:**
     - Use **Delete Records** and select **High Watermark**.

---
### Start interactive container.

Follow guide in [interactive containers](./interactive/README.md) to start an interactive container.




