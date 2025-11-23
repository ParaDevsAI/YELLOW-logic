import logging
import re
import os
import httpx
from telegram import Update, ReplyKeyboardRemove
from telegram.ext import (
    ConversationHandler, 
    CommandHandler, 
    MessageHandler, 
    filters, 
    ContextTypes
)
from author_manager import is_author_registered, register_author

logger = logging.getLogger(__name__)

# States for the ConversationHandler
ASKING_TWITTER_USERNAME = 1

async def fetch_twitter_user_data(username: str) -> dict | None:
    """Fetch real Twitter user data from Twitter API."""
    api_key = os.getenv("TWITTER_API_KEY")
    if not api_key:
        logger.error("TWITTER_API_KEY não encontrada")
        return None
    
    try:
        url = "https://api.twitterapi.io/twitter/user/info"
        headers = {"x-api-key": api_key}
        params = {"userName": username}
        
        logger.info(f"Buscando dados do usuário @{username} via Twitter API...")
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            # The API wraps results under a top-level 'data' key. Normalize it.
            payload = data.get('data') if isinstance(data, dict) and 'data' in data else data
            if payload and isinstance(payload, dict):
                logger.info(f"Dados do Twitter obtidos para @{username}")
                return {
                    'id': payload.get('id'),
                    'userName': payload.get('userName') or payload.get('userName') or payload.get('userName'),
                    'name': payload.get('name'),
                    'description': payload.get('description', ''),
                    'followers': payload.get('followers', 0),
                    'following': payload.get('following', 0),
                    'statusesCount': payload.get('statusesCount') or payload.get('statusesCount', 0),
                    'mediaCount': payload.get('mediaCount', 0),
                    'createdAt': payload.get('createdAt'),
                    'isBlueVerified': payload.get('isBlueVerified', False),
                    'profilePicture': payload.get('profilePicture')
                }
            else:
                logger.warning(f"Usuário @{username} não encontrado no Twitter")
                return None
        else:
            logger.error(f"Erro da API Twitter para @{username}: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Erro ao buscar dados do Twitter para @{username}: {e}")
        return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /start command and begin registration flow."""
    user = update.effective_user
    telegram_id = user.id
    
    # Check if user is already registered
    is_registered = await is_author_registered(telegram_id)
    
    if is_registered:
        await update.message.reply_text(
            f"Olá {user.first_name}! Você já está registrado no sistema YELLOW. ✅\n\n"
            "Use o bot normalmente para rastrear seus tweets e ganhar pontos!"
        )
        return ConversationHandler.END
    
    # Start registration process
    await update.message.reply_text(
        f"Olá {user.first_name}! 👋\n\n"
        "Bem-vindo ao sistema YELLOW de rastreamento de engajamento!\n\n"
        "Para começar, preciso do seu nome de usuário do Twitter (sem o @).\n"
        "Por exemplo, se seu perfil é @joaosilva, digite apenas: joaosilva\n\n"
        "Digite seu nome de usuário do Twitter:"
    )
    
    return ASKING_TWITTER_USERNAME

async def receive_twitter_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive and validate Twitter username, then register the user."""
    user = update.effective_user
    twitter_username = update.message.text.strip()
    
    # Clean up the username (remove @ if present)
    twitter_username = twitter_username.lstrip('@')
    
    # Basic validation
    if not re.match(r'^[A-Za-z0-9_]{1,15}$', twitter_username):
        await update.message.reply_text(
            "❌ Nome de usuário inválido!\n\n"
            "O nome de usuário do Twitter deve conter apenas letras, números e underscore (_), "
            "e ter no máximo 15 caracteres.\n\n"
            "Digite novamente:"
        )
        return ASKING_TWITTER_USERNAME
    
    # Show processing message
    processing_msg = await update.message.reply_text("🔄 Buscando dados do Twitter...")
    
    # Fetch real Twitter user data
    twitter_data = await fetch_twitter_user_data(twitter_username)
    
    if not twitter_data:
        await processing_msg.delete()
        await update.message.reply_text(
            f"❌ Não foi possível encontrar o usuário @{twitter_username} no Twitter!\n\n"
            "Verifique se:\n"
            "• O nome de usuário está correto\n"
            "• A conta não está privada ou suspensa\n"
            "• Você digitou apenas o nome de usuário (sem @)\n\n"
            "Digite novamente:"
        )
        return ASKING_TWITTER_USERNAME
    
    # Try to register the author
    success = await register_author(user, twitter_data)
    
    await processing_msg.delete()
    
    if success:
        twitter_name = twitter_data.get('name', twitter_username)
        followers_count = twitter_data.get('followers', 0)
        verification_status = "✅ Verificado" if twitter_data.get('isBlueVerified') else ""
        
        await update.message.reply_text(
            f"🎉 Registro concluído com sucesso!\n\n"
            f"✅ Telegram: {user.first_name}\n"
            f"✅ Twitter: @{twitter_username}\n"
            f"📝 Nome: {twitter_name}\n"
            f"👥 Seguidores: {followers_count:,}\n"
            f"{verification_status}\n\n"
            "Agora você pode começar a usar o bot para rastrear seus tweets!\n"
            "Compartilhe links de tweets nos grupos YELLOW para ganhar pontos. 🚀",
            reply_markup=ReplyKeyboardRemove()
        )
        logger.info(f"Usuário {user.id} (@{twitter_username}) registrado com sucesso.")
    else:
        await update.message.reply_text(
            "❌ Erro ao registrar usuário!\n\n"
            "Houve um problema ao salvar seus dados. "
            "Tente novamente mais tarde ou contate um administrador.\n\n"
            "Digite /start para tentar novamente."
        )
        logger.error(f"Falha ao registrar usuário {user.id} com Twitter @{twitter_username}")
    
    return ConversationHandler.END

async def cancel_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the registration process."""
    await update.message.reply_text(
        "❌ Registro cancelado.\n\n"
        "Digite /start quando quiser se registrar.",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END

# Create the ConversationHandler
start_handler = ConversationHandler(
    entry_points=[CommandHandler("start", start)],
    states={
        ASKING_TWITTER_USERNAME: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_twitter_username)
        ],
    },
    fallbacks=[CommandHandler("cancel", cancel_registration)],
)
