package com.saga.account.service

import com.saga.account.domain.Account
import com.saga.account.domain.AccountTransaction
import com.saga.account.domain.SagaState
import com.saga.account.repository.AccountRepository
import com.saga.account.repository.AccountTransactionRepository
import com.saga.account.repository.SagaStateRepository
import com.saga.common.dto.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.client.RestTemplate
import java.math.BigDecimal
import java.util.*


@Service
class OrchestrationService(
    private val accountRepository: AccountRepository,
    private val accountTransactionRepository: AccountTransactionRepository,
    private val sagaStateRepository: SagaStateRepository,
    private val restTemplate: RestTemplate,
    @Value("\${service.transaction.url}") private val transactionServiceUrl: String,
    @Value("\${service.notification.url}") private val notificationServiceUrl: String
) {

    @Transactional
    fun executeTransfer(request : TransferRequest) : TransferResponse {
        val sagaId = UUID.randomUUID().toString()

        try {
            // 1. Withdraw from source account
            val fromAccount = accountRepository.findByAccountNumber(request.fromAccountNumber)
                ?: throw RuntimeException("Source account not found")

            if (fromAccount.balance < request.amount) {
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
                patternType = "ORCHESTRATION",
                fromAccountId = fromAccount.accountId,
                toAccountId = request.toAccountNumber,
                amount = request.amount,
                status = "STARTED"
            )
            sagaStateRepository.save(sagaState)


            try {
                // 다른 서비스를 호출

                val depositRequest = DepositRequest(
                    sagaId = sagaId,
                    accountNumber = request.toAccountNumber,
                    amount = request.amount,
                    fromAccountNumber = request.fromAccountNumber
                )

                restTemplate.postForObject(
                    "$transactionServiceUrl/internal/deposit",
                    depositRequest,
                    DepositResponse::class.java
                ) ?: throw RuntimeException("Deposit failed")

                sagaState.status = "COMPLETED"
                sagaStateRepository.save(sagaState)

                return TransferResponse(sagaId, "COMPLETED", "Transfer successful")

            } catch(e : Exception) {
                fromAccount.balance = fromAccount.balance.add(request.amount)
                accountRepository.save(fromAccount)

                withdrawTx.status = "COMPENSATED"
                accountTransactionRepository.save(withdrawTx)

                sagaState.status = "COMPENSATED"
                sagaStateRepository.save(sagaState)

                return TransferResponse(sagaId, "FAILED", "Deposit failed: ${e.message}")
            }

        } catch(e : Exception) {
            return TransferResponse(sagaId, "FAILED", e.message ?: "Unknown error")
        }

    }
}

