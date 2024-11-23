package cn.edu.ustb.producer.function;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 1. 实现ProducerInterceptor接口
 * <p>
 * 2. 定义泛型
 * </p>
 * 3. 重写方法
 */
public class ValueInterceptor implements ProducerInterceptor<String, String> {
    /**
     * 发送数据的时候，会调用此方法
     *
     * @param record the record from client or the record returned by the previous interceptor in the chain of interceptors.
     * @return 返回数据记录
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<>(record.topic(), record.key(), record.value() + ", " + record.value());
    }

    /**
     * 发送数据完毕后，服务器返回的响应
     *
     * @param metadata  The metadata for the record that was sent (i.e. the function and offset).
     *                  If an error occurred, metadata will contain only valid topic and maybe
     *                  function. If function is not given in ProducerRecord and an error occurs
     *                  before function gets assigned, then function will be set to RecordMetadata.NO_PARTITION.
     *                  The metadata may be null if the client passed null record to
     *                  {@link org.apache.kafka.clients.producer.KafkaProducer#send(ProducerRecord)}.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    /**
     * 生产者对象发送完数据后，会关闭，回收资源
     */
    @Override
    public void close() {

    }

    /**
     * 创建生产者对象的时候调用，传递相关配置
     *
     * @param configs 传递的配置项
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
