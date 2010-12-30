<?php

require_once 'amqplib/amqp.inc';

/**
 * @package Amqp
 * @author Matthew Hayes <Matthew.Hayes@AllThingsCode.com>
 */
final class Amqp_SimpleQueue
{
    /**
     * All properties for this object are stored in this array.
     * Default values are not populated,
     *   so if properties are accessed before they are set,
     *   a php notice is generated.  This behavior helps identify coding errors.
     * @var array
     */
    private $_properties = array();
    // ------------------------------------------------------------------------



    /**
     * constructor
     */
    public function __construct( $queueName )
    {
        $this->_setQueueName( $queueName );

        // Set some default values
        $this->setBrokerHost(     'localhost' );
        $this->setBrokerPort(      5672       );
        $this->setBrokerVhost(    '/'         );
        $this->setBrokerUsername( 'guest'     );
        $this->setBrokerPassword( 'guest'     );
    }



    /**
     * This helps prevent invalid property assignments.
     * @param string
     * @param mixed
     */
    public function __set( $propertyName, $propertyValue )
    {
        throw new Exception( 'Invalid property assignment: ' . $propertyName . ' => ' . $propertyValue );
    }
    /**
     * This helps catch invalid property retreival
     * @param string
     */
    public function __get( $propertyName )
    {
        throw new Exception( 'Invalid property retreival: ' . $propertyName );
    }



    // ----- Setters/Getters --------------------------------------------------

    /**
     * @param string
     */
    private function _setQueueName( $newValue )
    {
        $this->_properties['QueueName'] = $newValue;
    }
    /**
     * @return string
     */
    public function getQueueName()
    {
        return $this->_properties['QueueName'];
    }


    /**
     * @param string
     */
    public function setBrokerHost( $newValue )
    {
        $this->_properties['BrokerHost'] = $newValue;
    }
    /**
     * @return string
     */
    public function getBrokerHost()
    {
        return $this->_properties['BrokerHost'];
    }


    /**
     * @param string
     */
    public function setBrokerPort( $newValue )
    {
        $this->_properties['BrokerPort'] = $newValue;
    }
    /**
     * @return string
     */
    public function getBrokerPort()
    {
        return $this->_properties['BrokerPort'];
    }


    /**
     * @param string
     */
    public function setBrokerUsername( $newValue )
    {
        $this->_properties['BrokerUsername'] = $newValue;
    }
    /**
     * @return string
     */
    public function getBrokerUsername()
    {
        return $this->_properties['BrokerUsername'];
    }


    /**
     * @param string
     */
    public function setBrokerPassword( $newValue )
    {
        $this->_properties['BrokerPassword'] = $newValue;
    }
    /**
     * @return string
     */
    public function getBrokerPassword()
    {
        return $this->_properties['BrokerPassword'];
    }


    /**
     * @param string
     */
    public function setBrokerVhost( $newValue )
    {
        $this->_properties['BrokerVhost'] = $newValue;
    }
    /**
     * @return string
     */
    public function getBrokerVhost()
    {
        return $this->_properties['BrokerVhost'];
    }


    /**
     * @param string
     */
    private function _setConsumerBroker( $newValue )
    {
        $this->_properties['ConsumerBroker'] = $newValue;
    }
    /**
     * @return string
     */
    private function _getConsumerBroker()
    {
        if ( false === $this->_hasConsumerBroker() ) {
            $amqpBroker = new AMQPConnection(
                $this->getBrokerHost(),
                $this->getBrokerPort(),
                $this->getBrokerUsername(),
                $this->getBrokerPassword()
                );
            $this->_setConsumerBroker( $amqpBroker );
        }
        return $this->_properties['ConsumerBroker'];
    }
    /**
     * @return bool
     */
    private function _hasConsumerBroker()
    {
        $hasConsumerBroker = array_key_exists( 'ConsumerBroker', $this->_properties );
        return $hasConsumerBroker;
    }


    /**
     * @param string
     */
    private function _setConsumerChannel( $newValue )
    {
        $this->_properties['ConsumerChannel'] = $newValue;
    }
    /**
     * @return string
     */
    private function _getConsumerChannel()
    {
        if ( false === $this->_hasConsumerChannel() ) {
            $amqpBroker  = $this->_getConsumerBroker();
            $amqpChannel = $amqpBroker->channel();
            $this->_setConsumerChannel( $amqpChannel );
        }
        return $this->_properties['ConsumerChannel'];
    }
    /**
     * @return bool
     */
    private function _hasConsumerChannel()
    {
        $hasConsumerChannel = array_key_exists( 'ConsumerChannel', $this->_properties );
        return $hasConsumerChannel;
    }
    // ------------------------------------------------------------------------




    // ----- Public Methods ---------------------------------------------------

    /**
     * @param string
     * @param array
     */
    public function enqueue( $message, array $messageOptions )
    {
        $amqpBroker = new AMQPConnection(
            $this->getBrokerHost(),
            $this->getBrokerPort(),
            $this->getBrokerUsername(),
            $this->getBrokerPassword()
            );
        $amqpChannel = $amqpBroker->channel();
        $amqpChannel->access_request( $this->getBrokerVhost(), false, false, true, true );

        $amqpChannel->queue_declare( $this->getQueueName(), false, false, false, false );

        $amqpMessage = new AMQPMessage( $message, $messageOptions );

        $amqpChannel->basic_publish( $amqpMessage, '', $this->getQueueName() );

        $amqpChannel->close();
        $amqpBroker->close();
    }


    /**
     * @return AMQPMessage|NULL
     */
    public function getNextMessage()
    {
        $amqpChannel = $this->_getConsumerChannel();

        $amqpChannel->queue_declare( $this->getQueueName(), false, false, false, false );

        $message = $amqpChannel->basic_get( $this->getQueueName() );

        return $message;
    }


    /**
     * @param int This should be the delivery_info['delivery_tag'] value of the processed message.
     */
    public function dequeue( $messageId )
    {
        $amqpChannel = $this->_getConsumerChannel();
        $amqpChannel->basic_ack( $messageId );
    }


    /**
     * It's polite to explicitly close connections.
     */
    public function close()
    {
        if ( true === $this->_hasConsumerChannel() ) {

            $amqpChannel = $this->_getConsumerChannel();
            $amqpChannel->close();

            $amqpBroker = $this->_getConsumerBroker();
            $amqpBroker->close();
        }
    }
    // ------------------------------------------------------------------------




    // ----- Private Methods --------------------------------------------------


    // ------------------------------------------------------------------------
}

