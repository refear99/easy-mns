<?php

namespace Refear99\EasyMns;

use AliyunMNS\Requests\BatchReceiveMessageRequest;
use AliyunMNS\Requests\SendMessageRequest;

class Queue extends Mns
{
    /**
     * 发送消息到 Queue
     *
     * @param string $queueName
     * @param array|string $message
     * @param null $delay
     * @param null $priority
     * @param bool $base64
     * @return array
     * @throws \Exception
     */
    public function sendMessage($queueName, $message, $delay = null, $priority = null, $base64 = true)
    {
        try {
            if (is_array($message)) {
                $message = \GuzzleHttp\json_encode($message);
            } else if (is_string($message)) {
                $message = trim($message);
            } else {
                throw new \Exception('Invalid Message');
            }

            $request = new SendMessageRequest($message, $delay, $priority, $base64);

            $result = $this->mns->getQueueRef($queueName)->sendMessage($request);

            if (!$result->isSucceed()) {
                throw new \Exception('Send Message Failed. Status Code ' . $result->getStatusCode());
            }

            return [
                'message_status_code' => $result->getStatusCode(),
                'message_id'          => $result->getMessageId(),
                'message_body_md5'    => $result->getMessageBodyMD5(),
            ];

        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * 接收消息
     *
     * @param string $queueName
     * @param int $waitSec
     * @return array
     * @throws \Exception
     */
    public function receiveMessage($queueName, $waitSec = 30)
    {
        try {
            $result = $this->mns->getQueueRef($queueName)->receiveMessage($waitSec);

            if (!$result->isSucceed()) {
                throw new \Exception('Receive Message Failed. Status Code ' . $result->getStatusCode());
            }

            return [
                'message_status_code' => $result->getStatusCode(),
                'message_id'          => $result->getMessageId(),
                'message_body'        => $result->getMessageBody(),
                'message_body_md5'    => $result->getMessageBodyMD5(),
                'receipt_handle'      => $result->getReceiptHandle(),
                'enqueue_time'        => $result->getEnqueueTime(),
                'next_visible_time'   => $result->getNextVisibleTime(),
                'first_dequeue_time'  => $result->getFirstDequeueTime(),
                'dequeue_count'       => $result->getDequeueCount(),
                'priority'            => $result->getPriority(),
            ];

        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * 批量接收消息
     *
     * @param string $queueName
     * @param int $number
     * @param int $waitSec
     * @return array
     * @throws \Exception
     */
    public function receiveBatchMessage($queueName, $number = 16, $waitSec = 30)
    {
        try {

            $request = new BatchReceiveMessageRequest($number, $waitSec);

            $result = $this->mns->getQueueRef($queueName)->batchReceiveMessage($request);

            if (!$result->isSucceed()) {
                throw new \Exception('Receive Message Failed. Status Code ' . $result->getStatusCode());
            }

            $data = [];

            foreach ($result->getMessages() as $message) {
                $data[] = [
                    'message_status_code' => $result->getStatusCode(),
                    'message_id'          => $message->getMessageId(),
                    'message_body'        => $message->getMessageBody(),
                    'message_body_md5'    => $message->getMessageBodyMD5(),
                    'receipt_handle'      => $message->getReceiptHandle(),
                    'enqueue_time'        => $message->getEnqueueTime(),
                    'next_visible_time'   => $message->getNextVisibleTime(),
                    'first_dequeue_time'  => $message->getFirstDequeueTime(),
                    'dequeue_count'       => $message->getDequeueCount(),
                    'priority'            => $message->getPriority(),
                ];
            }

            return $data;

        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * 删除队列中的消息
     *
     * @param string $queueName
     * @param string $receiptHandle
     * @return bool
     * @throws \Exception
     */
    public function deleteMessage($queueName, $receiptHandle)
    {
        try {
            $result = $this->mns->getQueueRef($queueName)->deleteMessage($receiptHandle);

            if (!$result->isSucceed()) {
                throw new \Exception('Delete Message Failed. Status Code ' . $result->getStatusCode());
            }

            return true;

        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * 批量删除消息
     *
     * @param string $queueName
     * @param array $receiptHandle
     * @return bool
     * @throws \Exception
     */
    public function deleteBatchMessage($queueName, array $receiptHandle)
    {
        try {
            $result = $this->mns->getQueueRef($queueName)->batchDeleteMessage($receiptHandle);

            if (!$result->isSucceed()) {
                throw new \Exception('Delete Message Failed. Status Code ' . $result->getStatusCode());
            }

            return true;

        } catch (\Exception $e) {
            throw $e;
        }
    }
}