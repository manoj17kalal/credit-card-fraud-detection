#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for alert configurations
"""

import os
import asyncio
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


async def test_telegram():
    """Test Telegram bot configuration"""
    try:
        import telegram
        
        token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not token or not chat_id:
            logger.error("Telegram token or chat ID not set in environment variables")
            return False
        
        logger.info(f"Using Telegram token: {token[:5]}...{token[-5:]}")
        logger.info(f"Using chat ID: {chat_id}")
        
        bot = telegram.Bot(token=token)
        bot_info = await bot.get_me()
        logger.info(f"Connected to Telegram bot: {bot_info.username}")
        
        # Send a test message
        message = "🔍 Test message from Credit Card Fraud Detection System"
        await bot.send_message(chat_id=chat_id, text=message)
        logger.info("Test message sent successfully!")
        return True
    except Exception as e:
        logger.error(f"Telegram test failed: {e}")
        return False


def test_email():
    """Test email configuration"""
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        smtp_server = os.getenv('SMTP_SERVER')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        username = os.getenv('SMTP_USERNAME')
        password = os.getenv('SMTP_PASSWORD')
        email_from = os.getenv('EMAIL_FROM', username)
        email_to = os.getenv('EMAIL_TO')
        
        if not all([smtp_server, smtp_port, username, password, email_from, email_to]):
            logger.error("Email configuration incomplete in environment variables")
            return False
        
        logger.info(f"Using SMTP server: {smtp_server}:{smtp_port}")
        logger.info(f"Using email: {email_from} -> {email_to}")
        
        # Create message
        msg = MIMEMultipart()
        msg['Subject'] = '🔍 Test Email from Credit Card Fraud Detection System'
        msg['From'] = email_from
        msg['To'] = email_to
        
        body = """This is a test email from the Credit Card Fraud Detection System.
        
If you received this email, your email configuration is working correctly.
        
No action is required."""
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Connect and send
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(username, password)
            server.send_message(msg)
        
        logger.info("Test email sent successfully!")
        return True
    except Exception as e:
        logger.error(f"Email test failed: {e}")
        return False


async def main():
    """Run all tests"""
    logger.info("Testing alert configurations...")
    
    # Test Telegram
    telegram_result = await test_telegram()
    
    # Test Email
    email_result = test_email()
    
    # Print summary
    logger.info("\n===== TEST RESULTS =====")
    logger.info(f"TELEGRAM: {'✅ PASSED' if telegram_result else '❌ FAILED'}")
    logger.info(f"EMAIL: {'✅ PASSED' if email_result else '❌ FAILED'}")
    
    if telegram_result or email_result:
        logger.info("\n✅ At least one alert channel is working!")
    else:
        logger.error("\n❌ All alert channels failed. Please check your configurations.")


if __name__ == "__main__":
    asyncio.run(main())