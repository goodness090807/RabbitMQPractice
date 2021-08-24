using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace Receive
{
    class Program
    {
        static void Main(string[] args)
        {
            RpcService();
        }

        /// <summary>
        /// 消息確認和持久化
        /// </summary>
        public static void MessageAcknowledgmentAndDurability()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // dirable設定為true就會使用消息持久化
                channel.QueueDeclare(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"接收到：{message}");
                    Thread.Sleep(message.Length * 1000);
                    // 這個是用來判斷訊息是否完成的，如果沒有判斷到這段，Receive程式就斷掉了，就會另外發給另一個Receive
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };
                // autoAck要設定為false，這樣才能手動消息確認
                channel.BasicConsume(queue: "task_queue", autoAck: false, consumer: consumer);
                // 記得要停止在這裡，不然會沒有被綁定
                Console.ReadLine();
            }
        }

        /// <summary>
        /// 此種方式沒有宣告柱列，接收方可以透過交換器取得，柱列用臨時的就好
        /// </summary>
        public static void ExchangeSetting()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // 宣告交換器
                channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);
                // 因為要接收消息，也不需要透過任何指定的柱列(Queue)，所以可以使用暫時的柱列
                var queueName = channel.QueueDeclare().QueueName;
                // 綁定柱列名稱和交換器，因為沒有指定路由，所以最後一個不需要
                channel.QueueBind(queue: queueName, exchange: "logs", routingKey: "");
                Console.WriteLine(" [*] 等待訊息中......");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] {0}", message);
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                // 記得要停止在這裡，不然會沒有被綁定
                Console.ReadLine();
            }
        }

        /// <summary>
        /// 設定交換器和路由，可以做指定的路由發送
        /// </summary>
        public static void ExchangeAndRoutingBind()
        {
            Console.Write("請輸入要監聽的路由，可打多個請用空格分開：");
            var enterValue = Console.ReadLine();

            string[] rotues = enterValue.Split(' ');

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // 要將type改成Direct的原因，是因為Fanout會讓所有的都能接收到
                channel.ExchangeDeclare(exchange: "direct_logs", type: ExchangeType.Direct);
                // 暫時的queue
                string queueName = channel.QueueDeclare().QueueName;
                // 迴圈跑綁定路由
                foreach(var item in rotues)
                {
                    channel.QueueBind(queue: queueName, exchange: "direct_logs", routingKey: item);
                }
                // 宣告consumer
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;
                    Console.WriteLine($"從路由：{routingKey}，取得：{message}資料");
                };
                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                Console.WriteLine($"正在監聽：{string.Join(",", rotues)}的路由");
                Console.ReadLine();
            }
        }

        public static void TopicExchange()
        {
            // # 代表任意單詞，ex：succ.# 或是 #，#代表全部都能接收，succ.#代表succ.後面不管是什麼都能接收
            // * 代表只能一個單詞 ex：succ.* 或是 *.succ，succ.*代表只有類似succ.data 這樣才能收到，*.succ代表只有類似data.succ 這樣才能收到
            Console.WriteLine("請輸入要監聽的路由，可打多個請用空格分開：");
            var enterValue = Console.ReadLine();

            string[] rotues = enterValue.Split(' ');

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // 最主要是這邊Type改成Topic
                channel.ExchangeDeclare(exchange: "topic_logs", type: ExchangeType.Topic);

                var queueName = channel.QueueDeclare().QueueName;

                // 迴圈跑綁定路由
                foreach (var item in rotues)
                {
                    channel.QueueBind(queue: queueName, exchange: "topic_logs", routingKey: item);
                }
                // 宣告consumer
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;
                    Console.WriteLine($"從路由：{routingKey}，取得：{message}資料");
                };
                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                Console.WriteLine($"正在監聽：{string.Join(",", rotues)}的路由");
                Console.ReadLine();
            }
        }

        public static void RpcService()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // 做一個rpc的柱列
                channel.QueueDeclare(queue: "rpc_queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
                // 設定公平分發
                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                // 建立consumer
                var consumer = new EventingBasicConsumer(channel);
                channel.BasicConsume(queue: "rpc_queue", autoAck: false, consumer: consumer);

                Console.WriteLine("等待RPC的request");

                // 建立接收監聽
                consumer.Received += (model, ea) =>
                {
                    // 要回傳的訊息
                    string response = null;

                    var body = ea.Body.ToArray();
                    var props = ea.BasicProperties;
                    var replyProps = channel.CreateBasicProperties();
                    // 這個是可以用來判斷請求的唯一值
                    replyProps.CorrelationId = props.CorrelationId;

                    try
                    {
                        // 取得訊息
                        var message = Encoding.UTF8.GetString(body);
                        int n = int.Parse(message);
                        // 訊息
                        Console.WriteLine($"輸入fib({message})");
                        // 取得費式數列的值
                        response = fib(n).ToString();
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine($"發生錯誤：{ex.Message}");
                        response = "";
                    }
                    finally
                    {
                        var responseBytes = Encoding.UTF8.GetBytes(response);
                        // 送回訊息，透過傳過來的props.ReplyTo，再傳回去
                        channel.BasicPublish(exchange: "", routingKey: props.ReplyTo, basicProperties: replyProps, body: responseBytes);
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                };


                Console.ReadLine();
            }
        }

        private static int fib(int n)
        {
            if (n == 0 || n == 1)
            {
                return n;
            }

            return fib(n - 1) + fib(n - 2);
        }
    }
}
