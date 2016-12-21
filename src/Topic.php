<?php

namespace Refear99\EasyMns;

use AliyunMNS\Client;
use AliyunMNS\Model\MessageAttributes;
use AliyunMNS\Model\SubscriptionAttributes;
use AliyunMNS\Requests\PublishMessageRequest;
use AliyunMNS\Exception\MnsException;
use AliyunMNS\Responses\PublishMessageResponse;

class Topic extends Mns
{
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * 发送消息到 Topic
     *
     * @param string $topicName
     * @param array|string $message
     * @param null $messageTag
     * @param null $messageAttributes
     * @return array
     * @throws \Exception
     */
    public function sendMessageToTopic($topicName, $message, $messageTag = null, $messageAttributes = null)
    {
        try {
            if (is_array($message)) {
                $message = \GuzzleHttp\json_encode($message);
            } else if (is_string($message)) {
                $message = trim($message);
            } else {
                throw new \Exception('Invalid Message');
            }

            if ($messageAttributes) {
                $messageAttributes = new MessageAttributes($messageAttributes);
            }

            $request = new PublishMessageRequest($message, $messageTag, $messageAttributes);

            $result = $this->mns->getTopicRef($topicName)->publishMessage($request);

            if (!$result->isSucceed()) {
                throw new \Exception('Send Message Failed. Status Code ' . $result->getStatusCode());
            }

            return [
                'message_status_code' => $result->getStatusCode(),
                'message_id'       => $result->getMessageId(),
                'message_body_md5' => $result->getMessageBodyMD5(),
            ];

        } catch (\Exception $e) {
            throw $e;
        }
    }
}