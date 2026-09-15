package com.saga.account.service

import com.saga.account.domain.AccountTransaction
import com.saga.account.domain.SagaState
import com.saga.account.repository.AccountRepository
import com.saga.account.repository.AccountTransactionRepository
import com.saga.account.repository.SagaStateRepository
import com.saga.common.dto.TransferRequest
import com.saga.common.dto.TransferResponse
import com.saga.common.event.*
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.*

@Service
class ChoreographyService(
    private val accountRepository: AccountRepository,
    private val accountTransactionRepository: AccountTransactionRepository,
    private val sagaStateRepository: SagaStateRepository,
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {

    @Transactional
    fun initiateTransfer(request: TransferRequest) : TransferResponse {
        val sagaId = UUID.randomUUID().toString()

        try {
            val fromAccount = accountRepository.findByAccountNumber(request.fromAccountNumber)
                ?: throw RuntimeException("Source account not found")

            if (fromAccount.balance < request.amount) {
                val event = WithdrawFailedEvent(sagaId, request.fromAccountNumber, "Insufficient balance")
                kafkaTemplate.send("account.withdraw.failed", event)
                return TransferResponse(sagaId, "FAILED", "Insufficient balance")
            }

            fromAccount.balance = fromAccount.balance.subtract(request.amount)
            accountRepository.save(fromAccount)

            val withdrawTx = AccountTransaction(
                transactionId = UUID.randomUUID().toString(),
                accountId = fromAccount.accountId,
                amount = request.amount,
                transactionType = "WITHDRAW",
                sagaId = sagaId,
                status = "COMPLETED"
            )
            accountTransactionRepository.save(withdrawTx)

            val sagaState = SagaState(
                sagaId = sagaId,
                patternType = "CHOREOGRAPHY",
                fromAccountId = fromAccount.accountId,
                toAccountId = request.toAccountNumber,
                amount = request.amount,
                status = "STARTED"
            )
            sagaStateRepository.save(sagaState)

            val event = WithdrawSuccessEvent(
                sagaId = sagaId,
                accountNumber = request.fromAccountNumber,
                toAccountNumber = request.toAccountNumber,
                amount = request.amount
            )
            kafkaTemplate.send("account.withdraw.success", event)

            return TransferResponse(sagaId, "STARTED", "Transfer initiated")

        } catch (e : Exception) {
            val event = WithdrawFailedEvent(sagaId, request.fromAccountNumber, e.message ?: "Unknown error")
            kafkaTemplate.send("account.withdraw.failed", event)
            return TransferResponse(sagaId, "FAILED", e.message ?: "Unknown error")
        }
    }

    @KafkaListener(topics = ["transaction.deposit.failed"], groupId = "account-service-group")
    fun handleDepositFailed(event: DepositFailedEvent) {
        compensateWithdraw(event.sagaId)
    }

    @Transactional
    fun compensateWithdraw(sagaId: String) {
        val sagaState = sagaStateRepository.findById(sagaId).orElse(null) ?: return

        val fromAccount = accountRepository.findById(sagaState.fromAccountId).orElse(null) ?: return

        fromAccount.balance = fromAccount.balance.add(sagaState.amount)
        accountRepository.save(fromAccount)

        sagaState.status = "COMPENSATED"
        sagaStateRepository.save(sagaState)
    }

    @KafkaListener(topics = ["transaction.deposit.success"], groupId = "account-service-group")
    fun handleDepositSuccess(event: DepositSuccessEvent) {
        val sagaState = sagaStateRepository.findById(event.sagaId).orElse(null)
        sagaState?.let {
            it.status = "COMPLETED"
            sagaStateRepository.save(it)
        }
    }

    @KafkaListener(topics = ["notification.failed"], groupId = "account-service-group")
    fun handleNotificationFailed(event: NotificationFailedEvent) {
        println("[SAGA] Notification failed for saga ${event.sagaId}: ${event.reason}")

        val sagaState = sagaStateRepository.findById(event.sagaId).orElse(null)
        sagaState?.let {
            it.status = "COMPLETED_WITH_NOTIFICATION_FAILURE"
            sagaStateRepository.save(it)
        }
    }

}