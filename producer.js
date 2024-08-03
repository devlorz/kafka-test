const express = require("express");
const { Kafka } = require("kafkajs");
const { Order, Product, sequelize } = require("./schema");

const app = express();
const port = 8000;

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092", "localhost:9092"],
});

const producer = kafka.producer();

app.use(express.json());

app.post("/api/create-product", async (req, res) => {
  const productData = req.body;
  try {
    const product = await Product.create(productData);
    res.json(product);
  } catch (error) {
    res.json({
      message: "something wront",
      error,
    });
  }
});

app.post("/api/placeorder", async (req, res) => {
  const { userId, productId } = req.body;
  try {
    const product = await Product.findOne({ where: { id: productId } });

    if (product.amount <= 0) {
      res.status(400).json({ message: "Product out of stock" });
    }

    product.amount -= 1;
    await product.save();

    const order = await Order.create({
      userLineUid: userId,
      status: "pending",
      productId,
    });

    await producer.send({
      topic: "message-order",
      messages: [
        {
          value: JSON.stringify({
            userLineUid: userId,
            orderId: order.id,
          }),
        },
      ],
    });

    res.json({
      order,
      message: "Create order successfully",
    });
  } catch (error) {
    res.status(500).json({ error });
  }
});

app.listen(port, async () => {
  await sequelize.sync();
  await producer.connect();
  console.log(`Express app listening at http://localhost:${port}`);
});
