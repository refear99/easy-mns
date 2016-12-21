<?php

namespace Refear99\EasyMns;

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
    public function sendMessageToQueue($queueName, $message, $delay = null, $priority = null, $base64 = true)
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
}